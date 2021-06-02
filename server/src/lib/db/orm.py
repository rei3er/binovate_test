import sqlalchemy.engine as sqla_engine
from sqlalchemy import Column, Integer, String, ForeignKey, Text, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


_Base = declarative_base()


class User(_Base):
    __tablename__ = "User"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    group_chats = relationship("GroupChatMembers", backref="user")


class GroupChat(_Base):
    __tablename__ = "GroupChat"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    members = relationship("GroupChatMembers", backref="group_chat")


class P2PMessage(_Base):
    __tablename__ = "P2PMessage"

    id = Column(Integer, primary_key=True)
    message = Column(Text())
    origin_user_id = Column(Integer, ForeignKey(User.id))
    target_user_id = Column(Integer, ForeignKey(User.id))


class GroupChatMembers(_Base):
    __tablename__ = "GroupChatMembers"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey(User.id))
    group_chat_id = Column(Integer, ForeignKey(GroupChat.id))


class GroupChatMessage(_Base):
    __tablename__ = "GroupChatMessage"

    id = Column(Integer, primary_key=True)
    message = Column(Text())
    group_chat_member_id = Column(Integer, ForeignKey(GroupChatMembers.id))


def create_all(engine: sqla_engine.Engine) -> None:
    """
    Creates all tables.
    :param engine: Engine to use for creation.
    :return: None.
    """
    _Base.metadata.create_all(engine)


if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine("mysql+pymysql://mdu:mdu@127.0.0.1:3306/test")
    session_factory = sessionmaker(bind=engine)

    _Base.metadata.drop_all(engine)
    _Base.metadata.create_all(engine)

    with session_factory() as session:
        session.query(User).all()

