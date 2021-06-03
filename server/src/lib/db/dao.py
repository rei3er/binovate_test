import functools
import asyncio
import concurrent.futures
from typing import Callable, Hashable, Optional, Any, Union
import sqlalchemy
import sqlalchemy.engine as sqla_engine
import sqlalchemy.orm as sqla_orm
from . import orm


class DAO:
    AccessFunctionType = Callable[[Union[sqla_orm.Session, sqla_engine.Connection]], Any]

    def __init__(self,
                 url: str,
                 scoped: bool = True,
                 scopefunc: Optional[Callable[[], Hashable]] = None) -> None:
        """
        Initializes DAO instance.

        :param url: URL to access database.
        :param scoped: If True, use scoped session factory.
        :param scopefunc: Function that will be used by scoped session factory.
        :return: None.
        """

        self._executor = concurrent.futures.ThreadPoolExecutor()
        self._engine = sqlalchemy.create_engine(url)
        session_factory = sqla_orm.sessionmaker(self._engine)
        if scoped:
            session_factory = sqla_orm.scoped_session(session_factory, scopefunc=scopefunc)
        self._session_factory = session_factory
        self._init_schema()

    def _init_schema(self) -> None:
        """
        Creates all tables if they are not existed.

        :return: None.
        """

        orm.create_all(self._engine)

    def _access(self,
                func: AccessFunctionType,
                *args,
                with_session: bool = True,
                **kw) -> Any:
        """
        Calls func passing a Session or Connection to it.

        :param func: Callable to call.
        :param args: Arguments to pass to func.
        :param with_session: If True, use Session, otherwise Connection.
        :param kw: Key-value arguments to pass to func.
        :return: Any.
        """

        if with_session:
            with self._session_factory() as session:
                return func(session, *args, **kw)
        else:
            with self._engine.connect() as connection:
                return func(connection, *args, **kw)

    async def access(self, func: AccessFunctionType, *args, with_session: bool = True, **kw) -> Any:
        """
        Asynchronously accesses Session or Connection to do some actions.
        For example:

        def func(session, arg1, arg2):
            res = None
            # use session, set result to res
            ...
            return res
        ...
        dao = DAO(...)
        res = await dao.access(func, 1, 2)

        :param func: Any callable to use Session or Connection
        :param args: Arguments.
        :param with_session: Whether to use Session (True) or Connection (False).
        :param kw: Key-value arguments.
        :return: Any.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor,
                                          functools.partial(self._access, func, *args, with_session=with_session, **kw))
