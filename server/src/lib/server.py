import contextlib
import aiohttp.web as aw
from typing import Mapping, Any, Sequence
from .db import dao
from .db import orm


class Server:
    def __init__(self, cfg: Mapping[str, Any]) -> None:
        """
        Initializes server object.
        :param cfg: Configuration data. For simplicity assume it's valid and
                    contains all necessary data.
        """
        self._cfg = cfg
        self._app = None
        self._app_runner = None
        self._app_site = None
        self._engine = None
        self._session_factory = None
        self._started = False
        self._init_app()
        self._init_database_accessor()
        self._init_routes()

    def _init_app(self) -> None:
        """
        Creates aiohttp Application object.
        :return: None.
        """
        self._app = aw.Application()

    def _init_database_accessor(self) -> None:
        """
        Creates database engine and session factory.
        :return: None.
        """
        url = self._cfg.get("db.url")
        self._dao = dao.DAO(url)

    def _init_routes(self) -> None:
        self._app.router.add_get("/v1/messages", self._req_h_get_messages)

    async def _get_messages(self, uid: int, is_group: bool) -> Sequence[str]:
        def request(session):
            if is_group:
                cls = orm.GroupChatMessage
                cond = cls.id == uid
            else:
                cls = orm.P2PMessage
                cond = cls.origin_user_id == uid
            return [msg.message for msg in session.query(cls).filter(cond).all()]

        return await self._dao.access(request)

    async def _req_h_get_messages(self, request: aw.Request) -> aw.Response:
        """
        Processes GET request to /v1/messages. For simlicity let's assume
        the maximum number of messages in the DB is not to big to return
        them in a single JSON.

        Query format: ...?id=<user or group char id>[&group=<0 or 1>]

        :param request: Received request.
        :return: JSON response with the list of messages.
        """

        req_id = request.query.get("id")
        if req_id is None:
            raise aw.HTTPBadRequest(text="Missing 'id'")
        try:
            req_id = int(req_id)
        except ValueError:
            raise aw.HTTPBadRequest(text="Bad 'id': " + req_id)
        req_group = request.query.get("group", "0")
        try:
            req_group = bool(int(req_group))
        except ValueError:
            raise aw.HTTPBadRequest(text="Bad 'group': " + req_group)

        messages = await self._get_messages(req_id, req_group)

        return aw.json_response({"messages": messages})

    async def start(self) -> bool:
        """
        Starts server.
        :return: True if server has been started,
                 False if it was started previously.
        """
        res = not self._started
        if not self._started:
            self._app_runner = aw.AppRunner(self._app)
            await self._app_runner.setup()
            with contextlib.ExitStack() as stack:
                stack.callback(self._app_runner.cleanup)
                self._app_site = aw.TCPSite(self._app_runner,
                                            self._cfg.get("location.host"),
                                            self._cfg.get("location.port"),
                                            reuse_address=True)
                await self._app_site.start()
                self._started = True
                stack.pop_all()
        return res

    async def stop(self) -> None:
        """
        Stops server if it was started.
        :return: None.
        """
        if self._started:
            await self._app_runner.cleanup()

    @property
    def started(self) -> bool:
        return self._started
