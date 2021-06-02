import json
import contextlib
import aiohttp.web as aw
from typing import Mapping, Any, Sequence, Optional, Union
import sqlalchemy
import sqlalchemy.exc
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
        self._app.router.add_post("/v1/messages", self._req_h_post_messages)
        self._app.router.add_post("/v1/group_chat", self._req_h_post_group_chat)

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

    async def _post_messages(self,
                             message: str,
                             user_id: int,
                             target_id: int,
                             target_is_group_chat: bool) -> bool:
        def request(session):
            # for simplicity just return flag whether message was posted or not with
            # no additional information
            msg = None
            res = False
            if target_is_group_chat:
                member = session.query(orm.GroupChatMembers).filter(
                            sqlalchemy.and_(orm.GroupChatMembers.user_id == user_id,
                                            orm.GroupChatMembers.group_chat_id == target_id)).first()
                if member is None:
                    pass
                else:
                    msg = orm.GroupChatMessage(message=message,
                                               group_chat_member_id=member.id)
            else:
                msg = orm.P2PMessage(message=message,
                                     origin_user_id=user_id,
                                     target_user_id=target_id)
            if msg is not None:
                session.add(msg)

                try:
                    session.commit()
                except sqlalchemy.exc.IntegrityError:
                    pass
                else:
                    res = True

            return res

        return await self._dao.access(request)

    @staticmethod
    async def _get_request_body(request: aw.Request, body_is_json: bool = True) -> Union[str, Mapping[str, Any]]:
        if not request.body_exists:
            raise aw.HTTPBadRequest(text="No body")
        body = await request.text()
        try:
            data = json.loads(body) if body_is_json else body
        except ValueError:
            raise aw.HTTPBadRequest(text="Bad body: " + body)
        return data

    async def _req_h_post_group_chat(self, request: aw.Request) -> aw.Response:
        """
        Processes POST request to /v1/group_chat.

        JSON body format:
        {
            "name": <str>
        }

        JSON response format:
        {
            "status": <bool>,
            "group_chat_id": <int>
        }

        "group_char_id" will be missing if "status" is false.

        :param request: Received request.
        :return: JSON response with status and data.
        :raises:
            aw.HTTPBadRequest: If something is wong with the request.
        """

        data = await self._get_request_body(request)
        # for simplicity let's assume data is valid
        

    async def _req_h_post_messages(self, request: aw.Request) -> aw.Response:
        """
        Processes POST request to /v1/messages.

        JSON body format:
        {
            "originUserId": <int>,
            "targetId": <int>,
            "targetIsGroupChat": <bool>,
            "message": <str>
        }

        JSON response format:
        {
            "status": <bool>
        }

        :param request: Received request.
        :return: JSON response with status.
        :raises:
            aw.HTTPBadRequest: If something is wong with the request.
        """

        data = await self._get_request_body(request)
        # for simplicity let's assume data is valid
        post = await self._post_messages(data["message"],
                                         data["originUserId"],
                                         data["targetId"],
                                         data["targetIsGroupChat"])
        return aw.json_response({"status": post})

    async def _req_h_get_messages(self, request: aw.Request) -> aw.Response:
        """
        Processes GET request to /v1/messages. For simlicity let's assume
        the maximum number of messages in the DB is not to big to return
        them in a single JSON.

        Query format: ...?id=<user or group char id>[&group=<0 or 1>]

        :param request: Received request.
        :return: JSON response with the list of messages.
        :raises:
            aw.HTTPBadRequest: If something is wong with the request.
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
