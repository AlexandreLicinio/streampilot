import io
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streampilot"))
from streampilot import server


class ThemePageTests(unittest.TestCase):
    def assert_theme(self, body):
        body = body.decode() if isinstance(body, bytes) else body
        self.assertIn('src="/static/theme.js"', body)
        self.assertIn('href="/static/theme.css"', body)
        self.assertEqual(body.count('id="themeSwitch"'), 1)

    def test_template_and_standalone_pages_include_theme(self):
        response = SimpleNamespace(headers={})
        with patch.object(server.cherrypy, "response", response):
            self.assert_theme(server.App().login())
            self.assert_theme(server._gw_page("Gateway", "content"))
            self.assert_theme(server.render("dashboard.html", page_title="Dashboard", devices=[]))

    def test_import_error_pages_keep_theme_and_error(self):
        app = server.App()
        with patch.object(server.cherrypy, "session", {"username": "admin"}, create=True), \
             patch.object(server.cherrypy, "request", SimpleNamespace(method="POST")):
            for params, message in (({}, "Missing file"),
                                    ({"file": SimpleNamespace(file=io.StringIO("{"))}, "Invalid JSON")):
                with self.subTest(message=message):
                    body = app.log_import(**params)
                    self.assert_theme(body)
                    self.assertIn(message, body.decode())


if __name__ == "__main__":
    unittest.main()
