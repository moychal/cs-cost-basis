# Instructions
- Inspect and download data from csfloat API, can use adjustable limit. Make sure to start at page 0
    - https://csfloat.com/api/v1/me/trades?role=buyer&state=failed,cancelled,verified&limit=30&page=0
- Pull SCM purchases via extension: https://chromewebstore.google.com/detail/steam-market-history-cata/dhpcikljplaooekklhbjohojbjbinega
   - v.1.3.0
   - make sure to filter for purchases and download data
   - manually filter for year, extensions guesstimates year since steam does not provide it
- Pull data from skinport API https://skinport.com/api/checkout/order-history?page=1 and download
- Reference the downloads in parser.py and run

# TODO
- fix test data, currently it only works with my trades, which I .gitignored
- scrub trade_url, trade_token, username, steam_id from both buyer and sellers in csfloat jsons
- calculate p/l of sales
- maybe a UI and how/to with videos