import csv
import datetime
import json
from zoneinfo import ZoneInfo
from dataclasses import dataclass

### Instructions
# 1) Inspect and download data from csfloat API, can use adjustable limit. Make sure to start at page 0
# - https://csfloat.com/api/v1/me/trades?role=buyer&state=failed,cancelled,verified&limit=30&page=0
# 2) Put file names into jsons below
# 3) Pull SCM purchases via https://chromewebstore.google.com/detail/steam-market-history-cata/dhpcikljplaooekklhbjohojbjbinega
#   - v.1.3.0
# 4) Filtering for purchases via extension and download, then manually filtering because the extension guesstimates year.
# 5) Put file name into SCM_FILE_NAME below
# 6) Pull data from skinport API https://skinport.com/api/checkout/order-history?page=1 and add below
### Last run
# 2025 csfloat data: 3000_page0_trades, 3000_page1_trades, 3000_page2_trades
# scm data: scm_purchase_25.csv
# skinport data: skinport_trades.json

# Constants
STRIPE_FEE = .0288
SALES_TAX = .1035
PURCHASE_TIME_ZONE = 'America/Los_Angeles' # Seattle
CSFLOAT_FILE_NAMES = ['3000_page0_trades.json', '3000_page1_trades.json', '3000_page2_trades.json']
SCM_FILE_NAME = 'scm_purchase_25.csv'
SKINPORT_FILE_NAME = 'skinport_trades.json'
DEBUG = True

# Set these while parsing, and check at the end, to verify we didn't lose any
# during aggregation.
csf_price_parsed_sum = 0
csf_qty_parsed_sum = 0
scm_price_parsed_sum = 0
scm_qty_parsed_sum = 0
skinport_price_parsed_sum = 0
skinport_qty_parsed_sum = 0


aggregated_data = {}
@dataclass
class CSV_Tail:
  csf_qty: int = 0
  csf_price: int = 0
  stripe_fee: float = 0
  scm_qty: int = 0
  scm_price: int = 0
  skinport_qty: int = 0
  skinport_price: int = 0
  subtotal: int = 0
  sales_tax: float = 0
  total_cost: float = 0
  cost_basis: float = 0


def debug(str=None):
  if not DEBUG:
    return None
  else:
    return None if str is None else print(str)

# string must have have Z/explicitly be UTC
# e.g. "2025-10-28T13:31:13.648027Z"
def convert_iso_str_to_seattle_str(iso_string_utc):
  utc_timezone = ZoneInfo("UTC")

  dt_utc_aware = datetime.datetime.fromisoformat(iso_string_utc)
  assert dt_utc_aware.tzinfo == datetime.timezone.utc

  dt_seattle = dt_utc_aware.astimezone(ZoneInfo(PURCHASE_TIME_ZONE))
  return dt_seattle.strftime("%Y-%m-%d")

# Open the file and parse the JSON data
datas = []
for j in CSFLOAT_FILE_NAMES:
  with open(j, 'r') as file:
    datas.append(json.load(file))

faileds = 0
curr_parsed = 0
debug("Parsing")
for data in datas:
  # Iterate through the data (assuming it's a list of transactions)
  total = data['count']
  data = data['trades']
  for transaction in data:
    # Check if the required keys exist
    if "contract" in transaction and "item" in transaction["contract"] and \
        "market_hash_name" in transaction["contract"]["item"] and \
        "price" in transaction["contract"]:
      curr_parsed += 1
      if transaction["state"] != "verified":
        faileds += 1
        continue

      item_name = transaction["contract"]["item"]["market_hash_name"]
      # Price is represented in cents already
      price = transaction["contract"]["price"]
      date = convert_iso_str_to_seattle_str(transaction["accepted_at"])

      float_value = None if transaction["contract"]["item"]["is_commodity"] else transaction["contract"]["item"]["float_value"]


      # Aggregate by item_name, date, float_value
      k = (item_name, date, float_value)
      csf_price_parsed_sum += price
      csf_qty_parsed_sum += 1
      if k in aggregated_data:
        val = aggregated_data[k]
        val.csf_price = val.csf_price + price
        val.csf_qty = val.csf_qty + 1
      else:
        aggregated_data[k] = CSV_Tail(csf_price=price, csf_qty=1)
    else:
        debug("missing keys")
  if curr_parsed != len(data):
      debug('length disrepancy, probably from failed trades')
  else:
      debug(f"{curr_parsed} parsed")
debug(f"{total} expected per the API response")
assert total == curr_parsed
debug(f"{curr_parsed} parsed - failed {faileds} = {curr_parsed - faileds} valid ")

## Parse SCM and add those aggregates
with open(SCM_FILE_NAME, mode='r', newline='') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        if row[0] == "Index":
          continue
        name, price, listed_on, acted_on, qty = row[4:]
        if "2024" in acted_on:
          acted_on = "2025" + acted_on[4:]
        date = datetime.datetime.strptime(acted_on, "%Y-%d-%m")
        date = date.strftime("%Y-%m-%d")
        qty = int(qty)
        dollars, cents = price[1:].split('.') if '.' in price else (price, "00")
        price = int(dollars) * 100 + int(cents)
        scm_price_parsed_sum += price
        scm_qty_parsed_sum += qty

        key =  (name, date, None)
        if key not in aggregated_data:
          aggregated_data[key] = CSV_Tail(scm_qty=qty, scm_price=price)
        else:
          val = aggregated_data[key]
          val.scm_qty = val.scm_qty + qty
          val.scm_price = val.scm_price + price

## Parse skinport
# Open the file and parse the JSON data
data = None
with open(SKINPORT_FILE_NAME, 'r') as file:
  data = json.load(file)

curr_parsed = 0
expected_parsed = 0
debug("Parsing skinport")
for order in data['result']['orders']:
  expected_parsed += len(order['sales'])
  date = convert_iso_str_to_seattle_str(order["created"])
  for sale in order['sales']:
    curr_parsed += 1
    item_name = sale['marketHashName']
    # Price is represented in cents already
    price = sale['salePrice']
    float_value = sale['wear']

    # Aggregate by item_name, date, float_value
    k = (item_name, date, float_value)
    skinport_price_parsed_sum += price
    skinport_qty_parsed_sum += 1
    if k in aggregated_data:
      val = aggregated_data[k]
      val.skinport_price = val.skinport_price + price
      val.skinport_qty = val.skinport_qty + 1
    else:
      aggregated_data[k] = CSV_Tail(skinport_price=price, skinport_qty=1)
  if curr_parsed != len(data):
      debug('length disrepancy, probably from failed trades')
  else:
      debug(f"{curr_parsed} parsed")
debug(f"{expected_parsed} expected per the API response")
assert expected_parsed == curr_parsed

# Now that we've parsed CSFloat and SCM purchases, apply fees and add columns total and cost basis. These are still in Cents.
for k, tail in aggregated_data.items():
  val = aggregated_data[k]
  val.stripe_fee = STRIPE_FEE * val.csf_price

  # Sales tax not applicable to stripe fee on converting $ to CSFloat's USD balance
  val.subtotal = val.csf_price + val.scm_price + val.skinport_price
  val.sales_tax = SALES_TAX * val.subtotal
  val.total_cost = val.subtotal + val.sales_tax + val.stripe_fee
  val.cost_basis = val.total_cost / (val.csf_qty + val.scm_qty + val.skinport_qty)


# Recount some and grab totals
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
# Print CSV
debug()
debug("Printing CSV")
print("Name, Date, Float, CSF Qty, CSF Price, Stripe fee, SCM Qty, SCM Price, Skinport Qty, Skinport Price, Subtotal, Sales Tax, Total Cost, Cost Basis")
# Sort by name then date
for (item_name, date, float_val), tail in sorted(aggregated_data.items(), key = lambda x: (x[0], x[1])):
  # In Cents
  recount += tail.csf_qty + tail.scm_qty
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
  print(f"{item_name},{date},{float_val},{tail.csf_qty},{tail.csf_price / 100},{tail.stripe_fee / 100},{tail.scm_qty},{tail.scm_price / 100},{tail.skinport_qty}, {tail.skinport_price / 100},{tail.subtotal / 100},{tail.sales_tax / 100},{tail.total_cost / 100},{tail.cost_basis / 100}")

debug()

debug(f"CSF sum: ${csf_sum / 100}, CSF qty: {csf_qty_sum}, SCM sum: ${scm_sum / 100}, SCM qty: {scm_qty_sum}, Skinport sum: ${skinport_sum / 100}, Skinport qty: {skinport_qty_sum}")
assert csf_sum == csf_price_parsed_sum
assert csf_qty_sum == csf_qty_parsed_sum
assert scm_sum == scm_price_parsed_sum
assert scm_qty_sum == scm_qty_parsed_sum
assert skinport_sum == skinport_price_parsed_sum
assert skinport_qty_sum == skinport_qty_parsed_sum
debug(f"Total qty in CSV: {recount}, Subtotal: ${subtotal_sum / 100}, Total cost: ${(cost_sum) / 100}")
debug(f"Total fees: ${(stripe_fee_sum + sales_tax_sum) / 100}, Stripe fees: ${stripe_fee_sum / 100}, Sales tax: ${sales_tax_sum / 100}")
