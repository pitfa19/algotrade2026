import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT_CPP = ROOT / "bots" / "cpp" / "bot.cpp"
CMAKE = ROOT / "bots" / "cpp" / "CMakeLists.txt"


class BotCppStaticTests(unittest.TestCase):
    def test_cmake_has_valid_minimum_required_command(self):
        cmake = CMAKE.read_text()

        self.assertEqual(cmake.splitlines()[0], "cmake_minimum_required(VERSION 3.16)")

    def test_strategy_normalizes_cross_venue_instrument_ids(self):
        source = BOT_CPP.read_text()

        self.assertIn("base_symbol(", source)
        self.assertNotIn("nyse_set.count(symbol)", source)

    def test_strategy_uses_api_sides_and_ioc_orders(self):
        source = BOT_CPP.read_text()
        strategy_body = re.search(
            r"class SimpleStrategy.*?int main",
            source,
            flags=re.DOTALL,
        ).group(0)

        self.assertNotIn('"buy"', strategy_body)
        self.assertNotIn('"sell"', strategy_body)
        self.assertIn('"bid"', strategy_body)
        self.assertIn('"ask"', strategy_body)
        self.assertIn('"order_type"', source)
        self.assertIn('"ioc"', source)

    def test_cmake_builds_bot_cpp_target(self):
        cmake = CMAKE.read_text()

        self.assertIn("add_executable(bot_cpp bot.cpp)", cmake)


if __name__ == "__main__":
    unittest.main()
