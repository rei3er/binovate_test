import unittest
import sqlalchemy.exc as sqla_exc
import lib.server


class TestServerInitialization(unittest.TestCase):
    valid_urls = ("mysql+pymysql://user:password@localhost/db",
                  "mysql+pymysql://localhost/db",
                  "mysql+pymysql://localhost:3306/db")

    invalid_urls = ("mysql//user:password@localhost/db",
                    "mysql+pymysql://localhost:port/db",
                    "unknown://localhost:3306/db")

    def _do_test(self, url: str) -> None:
        lib.server.Server({"db.url": url})

    def test_valid_url(self) -> None:
        for url in self.valid_urls:
            self._do_test(url)

    def test_invalid_urls(self) -> None:
        for url in self.invalid_urls:
            with self.assertRaises((sqla_exc.ArgumentError, ValueError)):
                self._do_test(url)
