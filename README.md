# mgc-feed

Encrypted MGC (micro gold) market data for my trading agents.

A GitHub Action pulls MGC 1H bars from Databento on weekdays (~6:05 AM and ~5:25 PM ET),
rebuilds daily and 4H bars on the CME session, adds settlements and ATR(14), and commits
`mgc.json.enc` (AES-256, encrypted with the `FEED_PASSPHRASE` secret).

The data is licensed and is never committed in plain text. Only the encrypted file is public.

**Secrets:** `DATABENTO_API_KEY`, `FEED_PASSPHRASE`
