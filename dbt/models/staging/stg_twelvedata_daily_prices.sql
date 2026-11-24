select
    raw.payload:meta.symbol::string as symbol,
    prices.value:datetime::string::date as trade_date,
    prices.value:open::string::number(18, 6) as open_price,
    prices.value:high::string::number(18, 6) as high_price,
    prices.value:low::string::number(18, 6) as low_price,
    prices.value:close::string::number(18, 6) as close_price,
    prices.value:volume::string::number(38, 0) as volume,
    raw.source_file,
    raw.loaded_at
from {{ source('marketlens_raw', 'historical_prices') }} as raw,
     lateral flatten(input => raw.payload:values) as prices