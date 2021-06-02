import asyncio
import argparse
import lib.server


def main(args: argparse.Namespace) -> None:
    db_url = "mysql+pymysql://"
    if args.db_user_name and args.db_user_password:
        db_url += args.db_user_name + ":" + args.db_user_password + "@"
    db_url += (args.db_host or "127.0.0.1") + ":"
    db_url += str(args.db_port or 3306) + "/"
    db_url += args.db_name

    cfg = {
        "db.url": db_url,
        "location.host": args.host,
        "location.port": args.port
    }

    # for simplicity just start server and that's all, no error handling and so on
    server = lib.server.Server(cfg)
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(server.start(), loop=loop)
    loop.run_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", action="store", dest="host", type=str, required=False, default=None,
                        help="Hostname or IP to bind to")
    parser.add_argument("-p", "--port", action="store", dest="port", type=int, required=False, default=None,
                        help="Port to bind to")
    parser.add_argument("--db-host", action="store", dest="db_host", type=str, required=False, default="127.0.0.1",
                        help="Database location")
    parser.add_argument("--db-port", action="store", dest="db_port", type=int, required=False, default=3306,
                        help="Database port")
    parser.add_argument("-u", "--db-user-name", action="store", dest="db_user_name", type=str, required=False,
                        default=None, help="Database user name")
    parser.add_argument("--db-user-password", action="store", dest="db_user_password", type=str, required=False,
                        default=None, help="Database user password")
    parser.add_argument("-d", "--db-name", action="store", dest="db_name", type=str, required=True,
                        help="Database name")
    args = parser.parse_args()

    main(args)
