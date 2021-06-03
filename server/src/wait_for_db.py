import socket
import argparse
import time
import errno
import os
import re
from typing import Optional


def _check(host: str, port: int, timeout: Optional[int]) -> None:
    start = time.time()
    while timeout is None or (time.time() - start) <= timeout:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
        except OSError as exc:
            if exc.errno == errno.ECONNREFUSED:
                continue
            raise
        finally:
            sock.close()
        break
    else:
        raise Exception("Timeout: " + str(timeout))


def main(args: argparse.Namespace) -> None:
    _check(args.host, args.port, args.timeout)
    if args.command:
        cmd_args = re.split(r"\s+", args.command)
        os.execv(cmd_args[0], cmd_args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", dest="host", type=str, action="store", required=True)
    parser.add_argument("--port", dest="port", type=int, action="store", required=True)
    parser.add_argument("-t", "--timeout", dest="timeout", type=int, action="store", required=False)
    parser.add_argument("-c", "--command", dest="command", type=str, action="store", required=False)
    args = parser.parse_args()
    main(args)
