import csv
import io
import json
import os
import tempfile
import unittest
from collections import defaultdict
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from unittest import mock

import parser


REPO_ROOT = Path(__file__).resolve().parent
TEST_DIR = REPO_ROOT / "test"


@contextmanager
def working_directory(path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def build_csfloat_trade(
    *,
    item_name="Recoil Case",
    accepted_at="2025-03-15T18:24:54.961958+00:00",
    state="verified",
    price=40,
    float_value=0.25,
    is_commodity=True,
):
    return {
        "state": state,
        "accepted_at": accepted_at,
        "contract": {
            "price": price,
            "item": {
                "market_hash_name": item_name,
                "float_value": float_value,
                "is_commodity": is_commodity,
            },
        },
    }


def build_skinport_sale(
    *, item_name="Sticker | Lorena (Holo)", sale_price=125, wear=None
):
    return {
        "marketHashName": item_name,
        "salePrice": sale_price,
        "wear": wear,
    }


class ParserUnitTestCase(unittest.TestCase):
    def setUp(self):
        self.debug_patcher = mock.patch.object(parser, "DEBUG", False)
        self.debug_patcher.start()

    def tearDown(self):
        self.debug_patcher.stop()

    def make_aggregated_data(self):
        return defaultdict(parser.CSV_Tail)

    def write_json(self, directory, name, content):
        path = Path(directory) / name
        path.write_text(json.dumps(content), encoding="utf-8")
        return str(path)

    def write_csv_fixture(self, directory, name, rows):
        path = Path(directory) / name
        with path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        return str(path)

    def read_csv_rows(self, path):
        with open(path, "r", encoding="utf-8", newline="") as file:
            return list(csv.reader(file))


class TestConvertIsoStrToSeattleStr(ParserUnitTestCase):
    def test_converts_utc_timestamp_to_purchase_timezone_date(self):
        converted = parser.convert_iso_str_to_seattle_str(
            "2025-03-24T09:36:35.811079+00:00"
        )
        self.assertEqual(converted, "2025-03-24")

    def test_requires_explicit_utc_timezone(self):
        with self.assertRaises(AssertionError):
            parser.convert_iso_str_to_seattle_str("2025-03-24T09:36:35.811079")


class TestParseCSFloatData(ParserUnitTestCase):
    def test_aggregates_fixture_data_and_coerces_commodity_float_to_none(self):
        aggregated_data = self.make_aggregated_data()

        parser.parse_csfloat_data(
            aggregated_data, [str(TEST_DIR / "csfloat" / "test_page0_trades.json")]
        )

        eye_of_horus = aggregated_data[
            ("M4A4 | Eye of Horus (Factory New)", "2025-03-24", 0.01751287654042244)
        ]
        recoil_case = aggregated_data[("Recoil Case", "2025-03-15", None)]

        self.assertEqual(eye_of_horus.csf_qty, 1)
        self.assertEqual(eye_of_horus.csf_price, 241368)
        self.assertEqual(recoil_case.csf_qty, 5)
        self.assertEqual(recoil_case.csf_price, 140)

    def test_skips_non_verified_trades_after_counting_them_as_parsed(self):
        aggregated_data = self.make_aggregated_data()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = self.write_json(
                tmpdir,
                "csfloat.json",
                {
                    "count": 2,
                    "trades": [
                        build_csfloat_trade(price=40, state="verified"),
                        build_csfloat_trade(price=60, state="cancelled"),
                    ],
                },
            )

            parser.parse_csfloat_data(aggregated_data, [file_path])

        row = aggregated_data[("Recoil Case", "2025-03-15", None)]
        self.assertEqual(row.csf_qty, 1)
        self.assertEqual(row.csf_price, 40)

    def test_raises_when_trade_shape_does_not_match_expected_contract_fields(self):
        aggregated_data = self.make_aggregated_data()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = self.write_json(
                tmpdir,
                "broken_csfloat.json",
                {
                    "count": 1,
                    "trades": [
                        {
                            "state": "verified",
                            "accepted_at": "2025-03-24T09:36:35.811079+00:00",
                        }
                    ],
                },
            )

            with self.assertRaisesRegex(AssertionError, "Failed to parse 1 trades"):
                parser.parse_csfloat_data(aggregated_data, [file_path])


class TestParseSCMData(ParserUnitTestCase):
    def test_parses_fixture_rows_into_expected_keys(self):
        aggregated_data = self.make_aggregated_data()

        parser.parse_scm_data(
            aggregated_data, [str(TEST_DIR / "scm" / "scm_purchase_25.csv")]
        )

        revolution_case = aggregated_data[("Revolution Case", "2025-11-05", None)]
        lucas = aggregated_data[("Sticker | Lucas (Holo)", "2025-07-30", None)]

        self.assertEqual(revolution_case.scm_qty, 3)
        self.assertEqual(revolution_case.scm_price, 753)
        self.assertEqual(lucas.scm_qty, 2)
        self.assertEqual(lucas.scm_price, 142)

    def test_rewrites_2024_acted_on_dates_to_2025(self):
        aggregated_data = self.make_aggregated_data()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = self.write_csv_fixture(
                tmpdir,
                "scm.csv",
                [
                    [
                        "Index",
                        "Credit",
                        "Transaction ID",
                        "App ID",
                        "Name",
                        "Price",
                        "Listed On",
                        "Acted On",
                        "Amount",
                    ],
                    [
                        1,
                        0,
                        "1-2",
                        730,
                        "Recoil Case",
                        "$0.40",
                        "2024-15-03",
                        "2024-15-03",
                        2,
                    ],
                ],
            )

            parser.parse_scm_data(aggregated_data, [file_path])

        row = aggregated_data[("Recoil Case", "2025-03-15", None)]
        self.assertEqual(row.scm_qty, 2)
        self.assertEqual(row.scm_price, 40)


class TestParseSkinportData(ParserUnitTestCase):
    def test_aggregates_fixture_sales_by_name_date_and_float(self):
        aggregated_data = self.make_aggregated_data()

        parser.parse_skinport_data(
            aggregated_data, [str(TEST_DIR / "skinport" / "skinport_trades.json")]
        )

        lorena = aggregated_data[("Sticker | Lorena (Holo)", "2025-09-11", None)]
        recoil_case = aggregated_data[("Recoil Case", "2025-08-22", None)]

        self.assertEqual(lorena.skinport_qty, 2)
        self.assertEqual(lorena.skinport_price, 255)
        self.assertEqual(recoil_case.skinport_qty, 1)
        self.assertEqual(recoil_case.skinport_price, 40)

    def test_hits_expected_parsed_assertion_with_minimal_valid_order(self):
        aggregated_data = self.make_aggregated_data()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = self.write_json(
                tmpdir,
                "skinport.json",
                {
                    "result": {
                        "orders": [
                            {
                                "created": "2025-09-11T15:12:34.567+00:00",
                                "sales": [
                                    build_skinport_sale(
                                        item_name="Sticker | Lorena (Holo)",
                                        sale_price=125,
                                    ),
                                    build_skinport_sale(
                                        item_name="Sticker | Lorena (Holo)",
                                        sale_price=130,
                                    ),
                                ],
                            }
                        ]
                    }
                },
            )

            parser.parse_skinport_data(aggregated_data, [file_path])

        row = aggregated_data[("Sticker | Lorena (Holo)", "2025-09-11", None)]
        self.assertEqual(row.skinport_qty, 2)
        self.assertEqual(row.skinport_price, 255)


class TestWriteCsv(ParserUnitTestCase):
    def test_writes_sorted_rows_and_serializes_none_float_values(self):
        aggregated_data = self.make_aggregated_data()
        aggregated_data[("B Item", "2025-01-02", 0.5)] = parser.CSV_Tail(
            csf_qty=1, csf_price=100
        )
        aggregated_data[("A Item", "2025-01-01", None)] = parser.CSV_Tail(
            scm_qty=2, scm_price=250
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.csv"
            parser.write_csv(aggregated_data, output_file=str(output_file))
            rows = self.read_csv_rows(output_file)

        self.assertEqual(rows[0][0:4], ["Name", "Date", "Float", "CSF Qty"])
        self.assertEqual(rows[1][0:3], ["A Item", "2025-01-01", "None"])
        self.assertEqual(rows[2][0:3], ["B Item", "2025-01-02", "0.5"])
        self.assertEqual(rows[1][6:8], ["2", "2.5"])
        self.assertAlmostEqual(float(rows[2][13]), 1.1323)


class TestWriteSummaryCsv(ParserUnitTestCase):
    def test_groups_rows_by_item_name(self):
        aggregated_data = self.make_aggregated_data()
        aggregated_data[("Item A", "2025-01-01", None)] = parser.CSV_Tail(
            csf_qty=1, csf_price=100
        )
        aggregated_data[("Item A", "2025-01-02", 0.4)] = parser.CSV_Tail(
            scm_qty=2, scm_price=250, skinport_qty=1, skinport_price=50
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "summary_output.csv"
            parser.write_summary_csv(aggregated_data, output_file=str(output_file))
            rows = self.read_csv_rows(output_file)

        self.assertEqual(
            rows[0][0:4], ["Name", "Total Qty", "Subtotal", "Pre-fee cost basis"]
        )
        self.assertEqual(rows[1][0], "Item A")
        self.assertEqual(rows[1][1], "4")
        self.assertAlmostEqual(float(rows[1][2]), 4.0)
        self.assertAlmostEqual(float(rows[1][3]), 1.0)
        self.assertAlmostEqual(float(rows[1][7]), 1.1107)


class TestWriteCasemoveCsv(ParserUnitTestCase):
    def test_writes_cost_basis_rows_for_casemove_import(self):
        aggregated_data = self.make_aggregated_data()
        aggregated_data[("Item A", "2025-01-01", None)] = parser.CSV_Tail(
            csf_qty=1, csf_price=100
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "casemove.csv"
            parser.write_casemove_csv(aggregated_data, output_file=str(output_file))
            rows = self.read_csv_rows(output_file)

        self.assertEqual(
            rows[0], ["Name", "Date", "Quantity", "Price", "Type", "Note", "Currency"]
        )
        self.assertEqual(
            rows[1][0:4], ["Item A", "2025-01-01", "1", "1.1322999999999999"]
        )
        self.assertEqual(rows[1][5:], ["From script", "USD"])


class TestRunner(unittest.TestCase):
    def run_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            os.symlink(TEST_DIR, Path(tmpdir) / "test", target_is_directory=True)

            buf = io.StringIO()
            with working_directory(tmpdir), redirect_stdout(buf):
                parser.runner("test")

            stdout = buf.getvalue()
            output_csv = (Path(tmpdir) / "output.csv").read_text(encoding="utf-8")
            expected_stdout = (Path(tmpdir) / "test" / "expected_stdout.txt").read_text(
                encoding="utf-8"
            )
            expected_csv = (Path(tmpdir) / "test" / "expected_output.csv").read_text(
                encoding="utf-8"
            )
            return stdout, output_csv, expected_stdout, expected_csv

    def test_stdout_matches_snapshot(self):
        stdout, _, expected_stdout, _ = self.run_snapshot()

        self.maxDiff = None
        self.assertEqual(
            stdout,
            expected_stdout,
            msg="runner stdout did not match expected_stdout.txt",
        )

    def test_csv_matches_snapshot(self):
        _, output_csv, _, expected_csv = self.run_snapshot()

        self.maxDiff = None
        self.assertEqual(
            output_csv, expected_csv, msg="output.csv did not match expected_output.csv"
        )

    def test_raises_when_no_input_files_are_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(AssertionError, "No files found"):
                parser.runner(tmpdir)


if __name__ == "__main__":
    unittest.main()
