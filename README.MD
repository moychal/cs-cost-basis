# How to aggregate
In `data/`, put csfloat, scm, and skinport jsons/csvs. 

Run the script via `python3 parser.py`. 

It outputs three summary csvs:
- `output.csv` for FIFO cost-basis aggregation
- `summary_output.csv` for aggregate cost-basis use in spreadsheets
- `casemove.csv` to upload to your Pricempire portfolio at https://pricempire.com/app/portfolio.

**This does not yet aggregate sales, nor match for tax-appropriate P/L reporting**

This assumes your timezone is in Seattle, and that you do not want to add csfloat's Stripe fees or Seattle-based sales tax to your cost-basis. Toggle `IGNORE_FEES` in `parser.py` if you are interested.

# How to get your trade data
## CSFloat
- Open browser dev tools, and look for the `trades` api call. It should look like:
    - https://csfloat.com/api/v1/me/trades?role=buyer&state=failed,cancelled,verified&limit=30&page=0
- Inspect the response's 'count' field to see how many JSONs you need to download (`total / limit`, I use `limit=3000` )
    - loop from `page=0` to `page=(total / limit) - 1` to consume all trades by your limit amount, I use limit=3000

## Steam Community Market
- Pull SCM purchases via extension: https://chromewebstore.google.com/detail/steam-market-history-cata/dhpcikljplaooekklhbjohojbjbinega
   - v.1.3.0
   - make sure to filter for purchases and download data
   - manually filter for year, extensions guesstimates year since steam does not provide it

## Skinport
- Pull data from skinport API https://skinport.com/api/checkout/order-history?page=1 and download
- Reference the downloads in parser.py and run

# TODO
- support sale aggregation
- calculate FIFO P/L for commodities, exact match for weapons
- support variable sales tax by year
- fix test data, currently it only works with my trades, which I .gitignored
- scrub trade_url, trade_token, username, steam_id from both buyer and sellers in csfloat jsons
- easier way to pull data, extensions?
- maybe a UI and how/to with videos