import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"


class PwaNavigationTemplateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        environment = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
        environment.globals["asset_version"] = lambda _path: "test"
        cls.html = environment.get_template("scanner.html").render()

    def test_more_menu_is_in_root_layer_with_alerts(self):
        self.assertEqual(self.html.count('id="menu-dropdown"'), 1)
        self.assertEqual(self.html.count('id="notif-settings-btn-mobile"'), 1)
        self.assertLess(self.html.index("</header>"), self.html.index('id="menu-dropdown"'))
        self.assertLess(
            self.html.index('id="menu-backdrop"'),
            self.html.index('id="menu-dropdown"'),
        )


if __name__ == "__main__":
    unittest.main()
