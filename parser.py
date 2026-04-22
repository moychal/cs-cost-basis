from collections import defaultdict
import csv
import datetime
import glob
import json
import os
from zoneinfo import ZoneInfo
from dataclasses import dataclass, field

### Instructions
# 1) Inspect and download data from csfloat API, can use adjustable limit. Make sure to start at page 0
# - https://csfloat.com/api/v1/me/trades?role=buyer&state=failed,cancelled,verified&limit=30&page=0
# - Place downloaded JSON files in data/csfloat/
# 2) Pull SCM purchases via https://chromewebstore.google.com/detail/steam-market-history-cata/dhpcikljplaooekklhbjohojbjbinega
#   - v.1.3.0
# 3) Filter for purchases via extension and download, then manually filter because the extension guesstimates year.
#   - Place downloaded CSV in data/scm/
# 4) Pull data from skinport API https://skinport.com/api/checkout/order-history?page=1
#   - Place downloaded JSON in data/skinport/

# Constants
IGNORE_FEES = False
STRIPE_FEE = 0.0288
SALES_TAX_RATES = {2025: 0.1035, 2026: 0.1055}
PURCHASE_TIME_ZONE = "America/Los_Angeles"  # Seattle
DEBUG = True

DATA_DIR = "data"


@dataclass
class CSV_Tail:
    date: datetime.datetime = None
    csf_qty: int = 0
    csf_price: int = 0
    scm_qty: int = 0
    scm_price: int = 0
    skinport_qty: int = 0
    skinport_price: int = 0
    ignore_fees: bool = False
    sales_tax_rate: dict = field(default_factory=dict)

    @property
    def subtotal(self):
        return self.csf_price + self.scm_price + self.skinport_price

    @property
    def sales_tax(self):
        # Sales tax not applicable to stripe fee, only for price of items
        year = self.date.year
        sales_tax = self.sales_tax_rate[year]
        return sales_tax * self.subtotal

    @property
    def stripe_fee(self):
        # Stripe fee only applied to transferring $ to csfloat balance
        return STRIPE_FEE * self.csf_price

    @property
    def total_cost(self):
        return self.subtotal + self.sales_tax + self.stripe_fee

    @property
    def total_qty(self):
        return self.csf_qty + self.scm_qty + self.skinport_qty

    @property
    def cost_basis(self):
        if self.total_qty > 0:
            if self.ignore_fees:
                return self.subtotal / self.total_qty
            else:
                return self.total_cost / self.total_qty
        return 0


def debug(str=None):
    if not DEBUG:
        return None
    else:
        return None if str is None else print(str)


# string must have have Z/explicitly be UTC
# e.g. "2025-10-28T13:31:13.648027Z"
# see https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
def convert_iso_str_to_seattle_str(iso_string_utc, purchase_time_zone):
    dt_utc_aware = datetime.datetime.fromisoformat(iso_string_utc)
    assert dt_utc_aware.tzinfo == datetime.timezone.utc

    dt_seattle = dt_utc_aware.astimezone(ZoneInfo(purchase_time_zone))
    return dt_seattle.strftime("%Y-%m-%d")


def parse_csfloat_data(
    aggregated_data, csfloat_files, purchase_time_zone
) -> defaultdict:
    datas = {}
    for j in csfloat_files:
        with open(j, "r") as file:
            datas[j] = json.load(file)

    faileds = 0
    curr_parsed = 0
    debug("\nParsing CSFloat data")
    for filename, data in datas.items():
        # Iterate through the data (assuming it's a list of transactions)
        total = data["count"]
        data = data["trades"]
        transactions_parsed = 0
        for transaction in data:
            if (
                "contract" in transaction
                and "item" in transaction["contract"]
                and "market_hash_name" in transaction["contract"]["item"]
                and "price" in transaction["contract"]
            ):
                curr_parsed += 1
                transactions_parsed += 1
                if transaction["state"] != "verified":
                    faileds += 1
                    continue

                item_name = transaction["contract"]["item"]["market_hash_name"]
                date = convert_iso_str_to_seattle_str(
                    transaction["accepted_at"], purchase_time_zone
                )

                float_value = (
                    None
                    if transaction["contract"]["item"]["is_commodity"]
                    else transaction["contract"]["item"]["float_value"]
                )

                # Aggregate by item_name, date, float_value
                k = (item_name, date, float_value)
                # Price is represented in cents already
                price = transaction["contract"]["price"]
                aggregated_data[k].date = datetime.datetime.strptime(date, "%Y-%m-%d")
                aggregated_data[k].csf_price += price
                aggregated_data[k].csf_qty += 1

        assert transactions_parsed == len(data), (
            f"Failed to parse {len(data) - transactions_parsed} trades from CSFloat API data in {filename}"
        )
        debug(f"{transactions_parsed}/{total} parsed from {filename}")
    assert total == curr_parsed
    debug(
        f"Parsed {curr_parsed}/{total} trades from CSFloat API data across {len(datas)} files, of which {faileds} failed trades were not included"
    )
    return aggregated_data


def parse_scm_data(aggregated_data, scm_files) -> defaultdict:
    parsed = 0
    debug("\nParsing SCM")
    for scm_file in scm_files:
        with open(scm_file, mode="r", newline="") as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                if row[0] == "Index":
                    continue
                parsed += 1
                name, price, listed_on, acted_on, qty = row[4:]
                if "2024" in acted_on:
                    acted_on = "2025" + acted_on[4:]
                date = datetime.datetime.strptime(acted_on, "%Y-%d-%m")
                date = date.strftime("%Y-%m-%d")
                qty = int(qty)
                dollars, cents = price[1:].split(".") if "." in price else (price, "00")
                price = int(dollars) * 100 + int(cents)

                key = (name, date, None)
                aggregated_data[key].date = datetime.datetime.strptime(date, "%Y-%m-%d")
                aggregated_data[key].scm_qty += qty
                aggregated_data[key].scm_price += price
            debug(f"{parsed} parsed from {scm_file}")
    return aggregated_data


def parse_skinport_data(
    aggregated_data, skinport_files, purchase_time_zone
) -> defaultdict:
    curr_parsed = 0
    expected_parsed = 0
    debug("\nParsing skinport")
    for skinport_file in skinport_files:
        with open(skinport_file, "r") as file:
            data = json.load(file)
            for order in data["result"]["orders"]:
                expected_parsed += len(order["sales"])
                date = convert_iso_str_to_seattle_str(
                    order["created"], purchase_time_zone
                )
                for sale in order["sales"]:
                    curr_parsed += 1
                    item_name = sale["marketHashName"]
                    # Price is represented in cents already
                    price = sale["salePrice"]
                    float_value = sale["wear"]

                    # Aggregate by item_name, date, float_value
                    k = (item_name, date, float_value)
                    aggregated_data[k].date = datetime.datetime.strptime(
                        date, "%Y-%m-%d"
                    )
                    aggregated_data[k].skinport_price += price
                    aggregated_data[k].skinport_qty += 1
            debug(f"{curr_parsed}/{expected_parsed} parsed from {skinport_file}")
    assert expected_parsed == curr_parsed
    debug(
        f"Parsed {curr_parsed}/{expected_parsed} from Skinport API data across {len(skinport_files)} files"
    )
    return aggregated_data


# should actually write to csv
def write_csv(aggregated_data, output_file="output/output.csv"):
    # Print CSV
    debug("\nPrinting CSV")
    with open(output_file, "w", encoding="utf-8") as file:
        writer = csv.writer(file)

        header = [
            "Name",
            "Date",
            "Float",
            "CSF Qty",
            "CSF Price",
            "Stripe fee",
            "SCM Qty",
            "SCM Price",
            "Skinport Qty",
            "Skinport Price",
            "Subtotal",
            "Sales Tax",
            "Total Cost",
            "Cost Basis",
        ]
        writer.writerow(header)

        # For summary rows
        recount = 0
        subtotal_sum = 0
        stripe_fee_sum = 0
        sales_tax_sum = 0
        cost_sum = 0
        csf_sum = 0
        csf_qty_sum = 0
        scm_sum = 0
        scm_qty_sum = 0
        skinport_sum = 0
        skinport_qty_sum = 0

        # Sort by name then date
        for (item_name, date, float_val), tail in sorted(
            aggregated_data.items(), key=lambda x: (x[0], x[1])
        ):
            # In Cents
            recount += tail.csf_qty + tail.scm_qty + tail.skinport_qty
            subtotal_sum += tail.subtotal
            stripe_fee_sum += tail.stripe_fee
            sales_tax_sum += tail.sales_tax
            cost_sum += tail.total_cost
            csf_sum += tail.csf_price
            csf_qty_sum += tail.csf_qty
            scm_sum += tail.scm_price
            scm_qty_sum += tail.scm_qty
            skinport_sum += tail.skinport_price
            skinport_qty_sum += tail.skinport_qty
            # Write data
            row = [
                item_name,
                date,
                float_val,
                tail.csf_qty,
                tail.csf_price / 100,
                tail.stripe_fee / 100,
                tail.scm_qty,
                tail.scm_price / 100,
                tail.skinport_qty,
                tail.skinport_price / 100,
                tail.subtotal / 100,
                tail.sales_tax / 100,
                tail.total_cost / 100,
                tail.cost_basis / 100,
            ]
            row = [
                str(x) for x in row
            ]  # writes 'None' for float values for commodities
            writer.writerow(row)

    debug(f"CSV written to {output_file}")
    debug("\n[Summary]")
    debug(
        f"CSF sum: ${csf_sum / 100}, CSF qty: {csf_qty_sum}, SCM sum: ${scm_sum / 100}, SCM qty: {scm_qty_sum}, Skinport sum: ${skinport_sum / 100}, Skinport qty: {skinport_qty_sum}"
    )
    debug(
        f"Total qty in CSV: {recount}, Subtotal: ${subtotal_sum / 100}, Total cost: ${(cost_sum) / 100}"
    )
    debug(
        f"Total fees: ${(stripe_fee_sum + sales_tax_sum) / 100}, Stripe fees: ${stripe_fee_sum / 100}, Sales tax: ${sales_tax_sum / 100}"
    )


def write_summary_csv(
    aggregated_data, ignore_fees, sales_tax, output_file="output/summary_output.csv"
):
    debug("\nPrinting summary CSV")
    summary = {}
    for (item_name, date, _), tail in aggregated_data.items():
        summary[item_name] = summary.get(
            item_name, CSV_Tail(ignore_fees=ignore_fees, sales_tax_rate=sales_tax)
        )
        summary[item_name].date = datetime.datetime.strptime(date, "%Y-%m-%d")
        summary[item_name].csf_qty += tail.csf_qty
        summary[item_name].csf_price += tail.csf_price
        summary[item_name].scm_qty += tail.scm_qty
        summary[item_name].scm_price += tail.scm_price
        summary[item_name].skinport_qty += tail.skinport_qty
        summary[item_name].skinport_price += tail.skinport_price

    with open(output_file, "w", encoding="utf-8") as file:
        writer = csv.writer(file)

        header = [
            "Name",
            "Total Qty",
            "Subtotal",
            "Pre-fee cost basis",
            "Stripe Fee",
            "Sales Tax",
            "Total Cost",
            "Cost Basis",
        ]
        writer.writerow(header)

        for item_name, tail in sorted(summary.items(), key=lambda x: x[1].total_cost):
            row = [
                item_name,
                tail.total_qty,
                tail.subtotal / 100,
                tail.subtotal / tail.total_qty / 100,
                tail.stripe_fee / 100,
                tail.sales_tax / 100,
                tail.total_cost / 100,
                tail.cost_basis / 100,
            ]
            writer.writerow(row)

    debug(f"Aggregate CSV written to {output_file}")


def write_casemove_csv(aggregated_data, output_file="output/casemove.csv"):
    debug("\nPrinting casemove CSV")

    with open(output_file, "w", encoding="utf-8") as file:
        writer = csv.writer(file)

        header = ["Name", "Date", "Quantity", "Price", "Type", "Note", "Currency"]
        writer.writerow(header)

        for (item_name, date, float_value), tail in sorted(
            aggregated_data.items(), key=lambda x: x[1].total_cost
        ):
            row = [
                item_name,
                date,
                tail.total_qty,
                tail.cost_basis / 100,
                "",
                "From script",
                "USD",
            ]
            writer.writerow(row)

    debug(f"Casemove CSV written to {output_file}")


def runner(
    input_file_dir=DATA_DIR,
    purchase_time_zone=PURCHASE_TIME_ZONE,
    sales_tax=SALES_TAX_RATES,
    ignore_fees=IGNORE_FEES,
):
    csf_file_names = sorted(
        glob.glob(os.path.join(input_file_dir, "csfloat", "*.json"))
    )
    scm_file_names = sorted(glob.glob(os.path.join(input_file_dir, "scm", "*.csv")))
    skinport_file_names = sorted(
        glob.glob(os.path.join(input_file_dir, "skinport", "*.json"))
    )

    if csf_file_names:
        debug(f"Discovered CSFloat files: {csf_file_names}")
    if scm_file_names:
        debug(f"Discovered SCM files: {scm_file_names}")
    if skinport_file_names:
        debug(f"Discovered Skinport files: {skinport_file_names}")
    assert csf_file_names or scm_file_names or skinport_file_names, (
        f"No files found, please place files in {DATA_DIR}/csfloat/, {DATA_DIR}/scm/, or {DATA_DIR}/skinport/"
    )

    aggregated_data = defaultdict(
        lambda: CSV_Tail(ignore_fees=ignore_fees, sales_tax_rate=sales_tax)
    )

    parse_csfloat_data(aggregated_data, csf_file_names, purchase_time_zone)
    parse_scm_data(aggregated_data, scm_file_names)
    parse_skinport_data(aggregated_data, skinport_file_names, purchase_time_zone)
    write_csv(aggregated_data)
    write_summary_csv(aggregated_data, ignore_fees, sales_tax)
    write_casemove_csv(aggregated_data)


if __name__ == "__main__":
    runner()
