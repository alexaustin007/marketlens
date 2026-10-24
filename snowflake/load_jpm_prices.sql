-- Fetches five JPM daily prices from Twelve Data, stages the JSON, and loads it into RAW.
CREATE OR REPLACE PROCEDURE MARKETLENS_DEV.RAW.LOAD_JPM_PRICES()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (MARKETLENS_TWELVEDATA_ACCESS)
SECRETS = ('twelvedata_key' = MARKETLENS_DEV.RAW.TWELVEDATA_API_KEY)
EXECUTE AS OWNER
AS
$$
import hashlib
import io
import json

import _snowflake
import requests


STAGE_PATH = "@MARKETLENS_DEV.RAW.MARKETLENS_RAW_STAGE/twelvedata/jpm/"


def run(session):
    api_key = _snowflake.get_generic_secret_string("twelvedata_key")
    response = requests.get(
        "https://api.twelvedata.com/time_series",
        params={
            "symbol": "JPM",
            "interval": "1day",
            "outputsize": 5,
            "apikey": api_key,
        },
        timeout=30,
    )
    if response.status_code != 200:
        raise ValueError(f"Twelve Data returned HTTP {response.status_code}")

    payload = response.json()
    if (
        payload.get("status") != "ok"
        or payload.get("meta", {}).get("symbol") != "JPM"
        or not isinstance(payload.get("values"), list)
        or not payload["values"]
    ):
        raise ValueError("Twelve Data did not return JPM daily price rows")

    raw_bytes = json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    filename = "jpm_1day_" + hashlib.sha256(raw_bytes).hexdigest() + ".json"

    session.file.put_stream(
        io.BytesIO(raw_bytes),
        STAGE_PATH + filename,
        auto_compress=False,
        overwrite=False,
    )

    copy_sql = f"""
        COPY INTO MARKETLENS_DEV.RAW.HISTORICAL_PRICES
          (PAYLOAD, SOURCE_FILE)
        FROM (
          SELECT t.$1, METADATA$FILENAME
          FROM {STAGE_PATH} t
        )
        FILES = ('{filename}')
        FILE_FORMAT = (TYPE = JSON)
    """
    copy_result = session.sql(copy_sql).collect()
    return f"Staged {filename}; COPY result: {copy_result}"
$$;
