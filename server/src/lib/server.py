import json
import contextlib
import aiohttp.web as aw
from typing import Mapping, Any, Sequence, Optional, Union, Tuple
import sqlalchemy
import sqlalchemy.orm as sqla_orm
import sqlalchemy.exc
from .db import dao
from .db import orm


class Server:
    CommonResponseType = Tuple[bool, Union[str, Any]]

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
        """
        Initializes API endpoints.

        :return: None.
        """

        self._app.router.add_get("/v1/messages", self._req_h_get_messages)
        self._app.router.add_post("/v1/messages", self._req_h_post_messages)
        self._app.router.add_post("/v1/user", self._req_h_post_user)
        self._app.router.add_post("/v1/group_chat", self._req_h_post_group_chat)
        self._app.router.add_post("/v1/group_chat/{id:[1-9]\\d*}/participants", self._req_h_post_add_to_group_chat)
        self._app.router.add_delete("/v1/group_chat/{gid:[1-9]\\d*}/participants/{uid:[1-9]\\d*}",
                                    self._req_h_post_del_from_group_chat)

    @staticmethod
    def _construct_common_response(status: bool, data: Any, error: str) -> CommonResponseType:
        """
        Constructs CommonResponseType.

        :param status: Status, True or False.
        :param data: Any data.
        :param error: Error string for the case when status is False.
        :return: CommonResponseType object.
        """
        return status, data if status else error

    async def _get_messages(self, uid: int, is_group: bool) -> CommonResponseType:
        """
        Gets all messages for the specified user or group chat.

        :param uid: User or group chat id.
        :param is_group: If True, uid is a group chat id.
        :return: CommonResponseType object.
        """

        def do(session: sqla_orm.Session) -> Optional[Sequence[str]]:
            # 1. Check if user or group chat exists.
            # 2. Query messages.
            cls, cond = (orm.User, orm.User.id == uid) if not is_group else (orm.GroupChat, orm.GroupChat.id == uid)
            obj = session.query(cls).filter(cond).first()
            if obj is not None:
                if not is_group:
                    res = session.query(orm.P2PMessage.message).filter(orm.P2PMessage.origin_user_id == uid).all()
                else:
                    res = session.query(orm.GroupChatMessage.message).join(orm.GroupChatMembers).filter(
                        orm.GroupChatMembers.group_chat_id == uid
                    ).all()
                res = tuple(t[0] for t in res)
            else:
                res = None
            return res

        messages = await self._dao.access(do)
        return self._construct_common_response(messages is not None, messages,
                                               "Identifier " + str(uid) + " does not exist")

    async def _post_messages(self,
                             message: str,
                             user_id: int,
                             target_id: int,
                             target_is_group_chat: bool) -> CommonResponseType:
        """
        Posts a message.

        :param message: Message to post.
        :param user_id: Id of user who posts the message.
        :param target_id: Id of group chat to post to or id of another user.
        :param target_is_group_chat: If True, target_id is a group chat id.
        :return: CommonResponseType object.
        """

        def do(session: sqla_orm.Session) -> Optional[str]:
            # 1. Check if user exists.
            # 2. If target_is_group_chat is False:
            #    - Check if target user exists.
            #    - Post the message to target user.
            # 3. If target_is_group_chat is True:
            #    - Check if target group chat exists.
            #    - Check if user is a member of this chat.
            #    - Post the message to target group chat.
            res, msg, invalid_id = None, None, None
            user = session.query(orm.User).filter(orm.User.id == user_id).first()
            if user is not None:
                if not target_is_group_chat:
                    target_user = session.query(orm.User).filter(orm.User.id == target_id).first()
                    if target_user is not None:
                        msg = orm.P2PMessage(message=message,
                                             origin_user_id=user_id,
                                             target_user_id=target_id)
                    else:
                        invalid_id = target_id
                else:
                    chat = session.query(orm.GroupChat).filter(orm.GroupChat.id == target_id).first()
                    if chat is not None:
                        member = session.query(orm.GroupChatMembers).filter(sqlalchemy.and_(
                            orm.GroupChatMembers.user_id == user_id,
                            orm.GroupChatMembers.group_chat_id == target_id
                        )).first()
                        if member is not None:
                            msg = orm.GroupChatMessage(message=message,
                                                       group_chat_member_id=member.id)
                        else:
                            res = "User " + str(user_id) + " is not a member of chat " + str(target_id)
                    else:
                        invalid_id = target_id
            else:
                invalid_id = user_id

            if msg is not None:
                session.add(msg)
                session.commit()
            elif res:
                pass
            else:
                res = "Identifier " + str(invalid_id) + " does not exist"

            return res

        error = await self._dao.access(do)
        return self._construct_common_response(error is None, None, error)

    async def _create_group_char(self, name: str) -> CommonResponseType:
        """
        Creates a new group chat.

        :param name: Group chat name.
        :return: CommonResponseType object.
        """

        def do(session: sqla_orm.Session) -> int:
            chat = orm.GroupChat(name=name)
            session.add(chat)
            session.commit()
            return chat.id

        res = await self._dao.access(do)
        return self._construct_common_response(True, res, None)

    async def _create_user(self, name: str) -> CommonResponseType:
        """
        Creates a new user.

        :param name: User name.
        :return: CommonResponseType object.
        """

        def do(session: sqla_orm.Session) -> int:
            chat = orm.User(name=name)
            session.add(chat)
            session.commit()
            return chat.id

        res = await self._dao.access(do)
        return self._construct_common_response(True, res, None)

    async def _add_to_group_chat(self, group_chat_id: int, users: Sequence[int], all_or_nothing: bool = False) -> \
            CommonResponseType:
        """
        Adds users to the group chat.

        :param group_chat_id: Group chat id.
        :param users: Sequence of user ids.
        :param all_or_nothing: If True, add all users or don't add at all in case of errors.
        :return: CommonResponseType object.
        """

        def do(session: sqla_orm.Session) -> Union[str, Sequence[int]]:
            # 1. Check if group chat exists.
            # 2. Get list of all users.
            # 3. Find unknown users and users which are already in the chat.
            # 4. Add specified users except unknown and that are already in the chat.
            chat = session.query(orm.GroupChat).filter(orm.GroupChat.id == group_chat_id).first()
            if chat is not None:
                all_users = set(t[0] for t in session.query(orm.User.id).all())
                add_users = set(users)
                unknown_users = add_users - all_users
                if unknown_users and all_or_nothing:
                    res = "Unknown identifiers [" + ",".join(map(str, unknown_users)) + "]"
                else:
                    add_users = add_users - unknown_users
                    ex_users = set(t[0] for t in session.query(orm.User.id).join(orm.GroupChatMembers).filter(
                        sqlalchemy.and_(
                            orm.User.id.in_(add_users),
                            orm.GroupChatMembers.group_chat_id == group_chat_id
                        )
                    ).all())
                    if ex_users and all_or_nothing:
                        res = "Users [" + ",".join(map(str, ex_users)) + "] are in chat " + str(group_chat_id)
                    else:
                        add_users = add_users - ex_users
                        for user_id in add_users:
                            session.add(orm.GroupChatMembers(user_id=user_id,
                                                             group_chat_id=group_chat_id))
                        session.commit()
                        res = tuple(add_users)
            else:
                res = "Unknown identifier " + str(group_chat_id)
            return res

        error_or_res = await self._dao.access(do)
        return self._construct_common_response(type(error_or_res) is not str, error_or_res, error_or_res)

    async def _del_from_group_chat(self, group_chat_id: int, user_id: int) -> CommonResponseType:
        """
        Deletes user with the specified id from the group chat with the specified id.

        :param group_chat_id: Group chat id.
        :param user_id: User id.
        :return: CommonResponseType object.
        """

        def do(session: sqla_orm.Session) -> Optional[str]:
            # 1. Check if group chat exists.
            # 2. Check if user exists.
            # 3. Check if user is a member of the chat.
            # 4. Deletes user from the chat.
            res = None
            chat = session.query(orm.GroupChat).filter(orm.GroupChat.id == group_chat_id).first()
            if chat is not None:
                user = session.query(orm.User).filter(orm.User.id == user_id).first()
                if user is not None:
                    member = session.query(orm.GroupChatMembers).filter(sqlalchemy.and_(
                        orm.GroupChatMembers.group_chat_id == group_chat_id,
                        orm.GroupChatMembers.user_id == user_id
                    )).first()
                    if member is not None:
                        session.delete(member)
                        session.commit()
                    else:
                        res = "User " + str(user_id) + " is not a member of chat " + str(group_chat_id)
                else:
                    res = "Unknown identifier " + str(user_id)
            else:
                res = "Unknown identifier " + str(group_chat_id)
            return res

        error = await self._dao.access(do)
        return self._construct_common_response(error is None, None, error)


    @staticmethod
    def _response4common(res: CommonResponseType) -> aw.Response:
        """
        Converts CommonResponseType to aw.Response.

        :param res: CommonResponseType object.
        :return: aw.Response object
        """

        return aw.json_response({
            "status": res[0],
            "data" if res[0] else "error": res[1]
        })

    @staticmethod
    async def _get_request_body(request: aw.Request, body_is_json: bool = True) -> Union[str, Mapping[str, Any]]:
        """
        Tries to get request body and convert it to JSON if necessary.

        :param request: Received request.
        :param body_is_json: Whether it's necessary to convert body to JSON.
        :return: Body string or JSON mapping.
        :raises:
            aw.HTTPBadRequest: If there is no body or it's invalid.
        """

        if not request.body_exists:
            raise aw.HTTPBadRequest(text="No body")
        body = await request.text()
        try:
            data = json.loads(body) if body_is_json else body
        except ValueError:
            raise aw.HTTPBadRequest(text="Bad body: " + body)
        return data

    async def _req_h_post_del_from_group_chat(self, request: aw.Request) -> aw.Response:
        """
        Processes DELETE request to /v1/group_chat/<gid>/participants/<uid>.

        Response format:

            {
                "status": <bool>,
                "data" or "error": null or <str>
            }

        :param request: Received request.
        :return: JSON response with status and data or an error.
        :raises:
            aw.HTTPBadRequest: If something is wong with the request.
        """
        group_chat_id = int(request.match_info.get("gid"))
        user_id = int(request.match_info.get("uid"))
        res = await self._del_from_group_chat(group_chat_id, user_id)

        return self._response4common(res)


    async def _req_h_post_add_to_group_chat(self, request: aw.Request) -> aw.Response:
        """
        Processes POST request to /v1/group_chat/<id>/participants.

        Request body format:

            {
                "userId: [<int>],
                "allOrNothing": <bool>
            }

        Response format:

            {
                "status": <bool>,
                "data" or "error": null or <str>
            }

        :param request: Received request.
        :return: JSON response with status and data or an error.
        :raises:
            aw.HTTPBadRequest: If something is wong with the request.
        """
        data = await self._get_request_body(request)
        group_chat_id = int(request.match_info.get("id"))
        # for simplicity let's assume data is valid
        res = await self._add_to_group_chat(group_chat_id,
                                            data["userId"],
                                            data["allOrNothing"])

        return self._response4common(res)

    async def _req_h_post_user(self, request: aw.Request) -> aw.Response:
        """
        Processes POST request to /v1/user.

        Request body format:

            {
                "name": <str>
            }

        Response format:

            {
                "status": <bool>,
                "data" or "error": <int> or <str>
            }

        :param request: Received request.
        :return: JSON response with status and data or an error.
        :raises:
            aw.HTTPBadRequest: If something is wong with the request.
        """

        data = await self._get_request_body(request)
        # for simplicity let's assume data is valid
        res = await self._create_user(data["name"])

        return self._response4common(res)

    async def _req_h_post_group_chat(self, request: aw.Request) -> aw.Response:
        """
        Processes POST request to /v1/group_chat.

        Request body format:

            {
                "name": <str>
            }

        Response format:

            {
                "status": <bool>,
                "data" or "error": <int> or <str>
            }

        :param request: Received request.
        :return: JSON response with status and data or an error.
        :raises:
            aw.HTTPBadRequest: If something is wong with the request.
        """

        data = await self._get_request_body(request)
        # for simplicity let's assume data is valid
        res = await self._create_group_char(data["name"])

        return self._response4common(res)


    async def _req_h_post_messages(self, request: aw.Request) -> aw.Response:
        """
        Processes POST request to /v1/messages.

        Request body format:

            {
                "originUserId": <int>,
                "targetId": <int>,
                "targetIsGroupChat": <bool>,
                "message": <str>
            }

        Response format:

             {
                "status": <bool>,
                "data" or "error": null or <str>
             }

        :param request: Received request.
        :return: JSON response with status and data or an error.
        :raises:
            aw.HTTPBadRequest: If something is wong with the request.
        """

        data = await self._get_request_body(request)
        # for simplicity let's assume data is valid
        res = await self._post_messages(data["message"],
                                        data["originUserId"],
                                        data["targetId"],
                                        data["targetIsGroupChat"])
        return self._response4common(res)

    async def _req_h_get_messages(self, request: aw.Request) -> aw.Response:
        """
        Processes GET request to /v1/messages. For simlicity let's assume
        the maximum number of messages in the DB is not to big to return
        them in a single JSON.

        Request query format:

            ...?id=<user or group chat id>[&group=<0 or 1>]

        Response format:

            {
                "status": <bool>,
                "data" or "error": [<str>] or <str>
            }

        :param request: Received request.
        :return: JSON response with status and data or an error.
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

        res = await self._get_messages(req_id, req_group)

        return self._response4common(res)

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
