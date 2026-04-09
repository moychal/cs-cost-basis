import unittest
import subprocess
import sys


class TestParser(unittest.TestCase):
    def test_runs_and_prints_header(self):
        proc = subprocess.run([sys.executable, 'parser.py'], capture_output=True, text=True, timeout=30)

        self.assertEqual(proc.returncode, 0, msg=f"parser.py exited with {proc.returncode}, stderr:\n{proc.stderr}")
        self.assertIn('Name, Date, Float, CSF Qty, CSF Price, Stripe fee, SCM Qty, SCM Price, Skinport Qty, Skinport Price, Subtotal, Sales Tax, Total Cost, Cost Basis', proc.stdout)

    def test_runs_and_prints_row(self):
        proc = subprocess.run([sys.executable, 'parser.py'], capture_output=True, text=True, timeout=30)
        
        self.assertEqual(proc.returncode, 0, msg=f"parser.py exited with {proc.returncode}, stderr:\n{proc.stderr}")
        self.assertIn('Recoil Case,2025-09-13,None,99,31.68,0.912384,0,0.0,0,0.0,31.68,3.2788799999999996,35.871264000000004,0.36233600000000005', proc.stdout)

    def test_stdout_matches_output_txt(self):
        proc = subprocess.run([sys.executable, 'parser.py'], capture_output=True, text=True, timeout=60)

        self.assertEqual(proc.returncode, 0, msg=f"parser.py exited with {proc.returncode}, stderr:\n{proc.stderr}")
        with open('test/expected_output.txt', 'r', encoding='utf-8') as f:
            expected = f.read()

        self.maxDiff = None
        self.assertEqual(proc.stdout, expected, msg='parser.py stdout did not match expected_output.txt')

        with open('output.csv', 'r', encoding='utf-8') as f:
            written_csv = f.read()
        with open('test/expected_output.csv', 'r', encoding='utf-8') as f:
            expected_csv = f.read()
        self.assertEqual(written_csv, expected_csv, msg='output.csv did not match expected_output.csv')


if __name__ == '__main__':
    unittest.main()