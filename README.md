# MarketLens

MarketLens is a financial data platform that brings together market prices, company filings, economic data, and news for research and analysis.

## Use the Python environment

From this folder, run:

```sh
source venv/bin/activate
```

Run `deactivate` when you are done.

## Configuration

Configuration comes from your Snowflake and data-provider accounts. Keep credentials and API keys in a local `.env` file. Do not commit `.env` or `venv/` to Git.

The project does not yet load `.env` automatically.

## Deploy the Snowflake ingestion procedure

The procedure source is in `snowflake/load_jpm_prices.sql`. It runs in Snowflake,
fetches five JPM daily prices from Twelve Data, puts the response in the internal
stage, and copies it into `RAW.HISTORICAL_PRICES`. Routine ingestion uses no
laptop file. This first version runs only when called; it is not scheduled yet.

The existing `MARKETLENS_DEV` RAW stage/table, Twelve Data network rule, API-key
secret, and external access integration must already exist in Snowflake. Their
non-secret setup is recorded in `snowflake/bootstrap.sql` for future clean
environments. Keep the actual API key in the Snowflake secret; never put it in
GitHub or this repo.

For a PR-based deployment, first publish an initial `main` commit. Then put the
SQL and workflow changes on a branch and open a PR. Before merging, create
the GitHub `marketlens-dev` environment with repository/environment secrets
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_ROLE`, and `SNOWFLAKE_WAREHOUSE`. The deploy role must be able to
create the procedure and use its integration and secret. Merging the PR into
`main` runs the GitHub Action, which executes the SQL file with Snowflake CLI.
The workflow deploys code; it does not call the API or load data.

After the Action succeeds, run this in a Snowflake worksheet to test one load:

```sql
CALL MARKETLENS_DEV.RAW.LOAD_JPM_PRICES();
```

Then check the loaded responses:

```sql
SELECT SOURCE_FILE, PAYLOAD:meta.symbol::STRING AS SYMBOL,
       ARRAY_SIZE(PAYLOAD:values) AS PRICE_ROWS, LOADED_AT
FROM MARKETLENS_DEV.RAW.HISTORICAL_PRICES
ORDER BY LOADED_AT DESC;
```
