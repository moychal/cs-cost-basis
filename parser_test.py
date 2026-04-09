import unittest
import subprocess
import sys


class TestParser(unittest.TestCase):
    # Should add tests for file discovery and progress and summary stats
    def test_stdout_matches(self):
        proc = subprocess.run([sys.executable, 'parser.py'], capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, msg=f"parser.py exited with {proc.returncode}, stderr:\n{proc.stderr}")
        
        with open('test/expected_stdout.txt', 'r', encoding='utf-8') as f:
            expected = f.read()

        self.maxDiff = None
        self.assertEqual(proc.stdout, expected, msg='parser.py stdout did not match expected_output.txt')

    def test_csv_matches(self):
        proc = subprocess.run([sys.executable, 'parser.py'], capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, msg=f"parser.py exited with {proc.returncode}, stderr:\n{proc.stderr}")

        self.maxDiff = None
        with open('output.csv', 'r', encoding='utf-8') as f:
            written_csv = f.read()
        with open('test/expected_output.csv', 'r', encoding='utf-8') as f:
            expected_csv = f.read()

        self.assertEqual(written_csv, expected_csv, msg='output.csv did not match expected_output.csv')


if __name__ == '__main__':
    unittest.main()