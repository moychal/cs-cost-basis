import io
import unittest
from contextlib import redirect_stdout

import parser


TEST_DIR = 'test'

# Snapshot tests
class TestParser(unittest.TestCase):
    # Should add tests for file discovery and progress and summary stats
    def _run(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            parser.runner(TEST_DIR)
        return buf.getvalue()

    def test_stdout_matches(self):
        stdout = self._run()

        with open('test/expected_stdout.txt', 'r', encoding='utf-8') as f:
            expected = f.read()

        self.maxDiff = None
        self.assertEqual(stdout, expected, msg='runner stdout did not match expected_stdout.txt')

    def test_csv_matches(self):
        self._run()

        self.maxDiff = None
        with open('output.csv', 'r', encoding='utf-8') as f:
            written_csv = f.read()
        with open('test/expected_output.csv', 'r', encoding='utf-8') as f:
            expected_csv = f.read()

        self.assertEqual(written_csv, expected_csv, msg='output.csv did not match expected_output.csv')


if __name__ == '__main__':
    unittest.main()
