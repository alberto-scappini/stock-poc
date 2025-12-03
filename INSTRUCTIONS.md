
# DBT Pipeline Instructions for ETF Feature Table

## 0. What we actually need (and what we can ignore)

From your raw tables:

### Keep / use:

- `eod_prices` → ETF prices, maybe FX too  
- `gbond_eod_prices` → government bond yields  
- `indx_eod_prices` → indices (VIX, DXY, SX5E, GSPC, etc.)  
- `div_eod_div` → ETF dividends  
- `fred_series_observations` → US macro & policy (CPIAUCSL, EFFR, SOFR, etc.)  
- `ecb_series_observations` → EU macro (HICP, Euribor, etc.)  
- `eod_symbols_metadata` or `supported_tickers_all_assets` → universe + meta (Name, Type, Country, Currency, Exchange)  

### Probably ignore for now:

- `supported_crypto_currencies`  
- `id_mapping_metadata` (nice to have, but not needed for features)  

---

## 1. Decide the grain of the final table

Final feature table should be:

- **Grain:** one row per **`(date, symbol)`**  
- Where `symbol` is an ETF you care about (plus maybe proxy US ETFs like `TLT`, `LQD`, `HYG`, etc.)

Example final model name: **`fct_etf_features_daily`**

Columns:

- `date`  
- `symbol`  
- other metadata (country, asset_class, is_bond, etc.)  
- price features  
- yield features (if applicable)  
- macro features (same across symbols per date)  
- bond-curve features (same across symbols per date)  

---

## 2. Build / reuse staging models (one per source table)

You said staging is already configured, so conceptually you have something like:

```sql
-- stg_eod_prices.sql
select
  date(date) as date,
  symbol,
  cast(open as float64)  as open,
  cast(high as float64)  as high,
  cast(low  as float64)  as low,
  cast(close as float64) as close,
  cast(adjusted_close as float64) as adjusted_close,
  cast(volume as int64)  as volume
from {{ source('raw', 'eod_prices') }}
```

Do the same for:

- `stg_gbond_eod_prices`  
- `stg_indx_eod_prices`  
- `stg_div_eod_div`  
- `stg_fred_series_observations`  
- `stg_ecb_series_observations`  
- `stg_eod_symbols_metadata` (or `stg_supported_tickers_all_assets`)  

Key things in staging:

- Parse dates: `date()` or `parse_date` from string fields.  
- Cast numerics to `float64`.  
- Drop `_airbyte_*` columns.  

---

## 3. Build a `dim_symbol` to hold your ETF universe & type labels

Create a dbt model: **`dim_symbol.sql`**

Use `eod_symbols_metadata` or `supported_tickers_all_assets` as base, filtered to just the symbols you care about (either by `Type = 'ETF'` + `Exchange` OR by a static list in a seed).

```sql
select
  Code       as symbol,
  Isin       as isin,
  Name       as name,
  Type       as asset_type,
  Country    as country,
  Currency   as currency,
  Exchange   as exchange,
  -- add manual classification:
  case
    when asset_type = 'ETF' and Name like '%Corp%' then 'IG_BOND'
    when asset_type = 'ETF' and Name like '%High Yield%' then 'HY_BOND'
    when asset_type = 'ETF' and Name like '%Treasury%' then 'TREASURY'
    when asset_type = 'ETF' and Name like '%Aggregate%' then 'AGG_BOND'
    when asset_type = 'ETF' then 'EQUITY'
    else 'OTHER'
  end as etf_group
from {{ ref('stg_eod_symbols_metadata') }}
where Code in (
  -- hardcode or join to a seed with your universe: TLT.US, IEAC.LSE, etc.
)
```

You can refine `etf_group` with a seed file mapping `symbol → group` if you want full control.

This `dim_symbol` is how you’ll:

- restrict your feature table to only symbols you want  
- know if a symbol is a bond vs equity etc.  

---

## 4. Create price-based mart for ETFs & indices

### 4.1 Prices for ETF symbols

`int_prices__etf_daily.sql`:

```sql
select
  p.date,
  p.symbol,
  p.open,
  p.high,
  p.low,
  p.close,
  p.adjusted_close,
  p.volume
from {{ ref('stg_eod_prices') }} p
join {{ ref('dim_symbol') }} s
  on p.symbol = s.symbol
```

Later you’ll use this to compute:

- returns  
- 3M / 6M momentum  
- volatility, etc.  

### 4.2 Prices for indices/risk proxies (VIX, SX5E, GSPC, etc.)

`int_prices__indices_daily.sql`:

```sql
select
  date,
  symbol,
  close
from {{ ref('stg_indx_eod_prices') }}
where symbol in ('VIX.INDX', 'DXY.INDX', 'SX5E.INDX', 'GSPC.INDX', 'NDX.INDX')
```

You’ll join these in as macro regime features.

---

## 5. Build dividend-based yield features (for distributing ETFs only)

### 5.1 Normalize dividends

`int_dividends__etf.sql`:

```sql
select
  date(paymentDate) as payment_date,
  symbol,
  cast(value as float64) as dividend_per_share,
  period
from {{ ref('stg_div_eod_div') }}
where symbol in (select symbol from {{ ref('dim_symbol') }})
```

You can keep `period` if you want for forward yield approximation later.

### 5.2 Daily dividend sums (to make rolling windows easier)

Sometimes easier to aggregate by day first:

`int_dividends__daily_sum.sql`:

```sql
select
  payment_date as date,
  symbol,
  sum(dividend_per_share) as div_per_share_day
from {{ ref('int_dividends__etf') }}
group by 1,2
```

### 5.3 Rolling 12M yield, 3M, 6M

In dbt/BigQuery, you can create a model `int_yield__etf_daily.sql` that joins prices + aggregated dividends and uses window functions:

```sql
with base as (
  select
    p.date,
    p.symbol,
    p.close,
    coalesce(d.div_per_share_day, 0.0) as div_per_share_day
  from {{ ref('int_prices__etf_daily') }} p
  left join {{ ref('int_dividends__daily_sum') }} d
    on p.date = d.date
   and p.symbol = d.symbol
),

div_roll as (
  select
    date,
    symbol,
    close,
    -- 12M trailing sum of dividends
    sum(div_per_share_day) over (
      partition by symbol
      order by date
      range between interval 365 day preceding and current row
    ) as div_12m,
    -- 3M trailing sum
    sum(div_per_share_day) over (
      partition by symbol
      order by date
      range between interval 90 day preceding and current row
    ) as div_3m,
    -- 6M trailing sum
    sum(div_per_share_day) over (
      partition by symbol
      order by date
      range between interval 180 day preceding and current row
    ) as div_6m
  from base
)

select
  date,
  symbol,
  close,
  div_12m,
  div_3m,
  div_6m,
  (case when close > 0 then div_12m / close end)                          as yield_12m_ttm,
  (case when close > 0 then div_3m  / close * (365.0 / 90.0)  end)        as yield_3m_ann,
  (case when close > 0 then div_6m  / close * (365.0 / 180.0) end)        as yield_6m_ann
from div_roll
```

This gives you a clean `int_yield__etf_daily` with:

- `yield_12m_ttm`  
- `yield_3m_ann`  
- `yield_6m_ann`  

For accumulating ETFs, `div_per_share_day` will be 0 and yields will be NULL/0.

---

## 6. Build bond yield mart from `gbond_eod_prices`

`int_yields__gov_bonds_daily.sql`:

```sql
select
  date(date) as date,
  symbol,              -- e.g. US10Y.GBOND, DE10Y.GBOND
  cast(close as float64) as yield
from {{ ref('stg_gbond_eod_prices') }}
where symbol in (
  'US10Y.GBOND', 'US5Y.GBOND', 'US3Y.GBOND', 'US1Y.GBOND',
  'DE10Y.GBOND', 'DE3Y.GBOND', 'DE1Y.GBOND',
  'IT10Y.GBOND', 'ES10Y.GBOND', 'PT10Y.GBOND'
)
```

You can later pivot or keep long, but usually keeping it long and then pivot during feature building is fine.

---

## 7. Build macro & policy marts from FRED + ECB

### 7.1 FRED – US CPI, EFFR, SOFR

Your `fred_series_observations` staging model should look like:

```sql
select
  date(date)              as date,
  cast(value as float64)  as value,
  series_id
from {{ source('raw', 'fred_series_observations') }}
```

Then create `int_macro__fred_monthly.sql`:

```sql
select
  date,
  series_id,
  value
from {{ ref('stg_fred_series_observations') }}
where series_id in ('CPIAUCSL', 'EFFR', 'SOFR')
```

Then:

- For `CPIAUCSL`, compute YoY in a separate model `int_macro__cpi_us_yoy.sql` using window functions (`lag 12`).  
- For `EFFR` and `SOFR`, you can either keep them monthly (and forward-fill to daily later) or use daily if you loaded daily frequency.

### 7.2 ECB – CPI EU YoY & Euribor

`stg_ecb_series_observations` normalized to:

```sql
select
  date(TIME_PERIOD)           as date,
  cast(OBS_VALUE as float64)  as value,
  KEY                         as key
from {{ source('raw', 'ecb_series_observations') }}
```

Then filter:

- `key = 'ICP.M.U2.N.000000.4.ANR'` → `CPI_EU_YOY`  
- `key like 'FM.M.U2.EUR.RT.MM.EURIBOR3MD%'` → `EURIBOR3M`  
- etc.

Create separate small models:

```sql
-- int_macro__cpi_eu_yoy.sql
select date, value as cpi_eu_yoy
from {{ ref('stg_ecb_series_observations') }}
where key = 'ICP.M.U2.N.000000.4.ANR'
```

Similarly for Euribor series.

Later, when you build the final table, you’ll join these monthly series to a **daily date spine** and forward-fill (last observation carried forward).

---

## 8. Build a date spine and base `(date, symbol)` grid

Model: **`int_spine__date_symbol.sql`**

```sql
with dates as (
  select
    date
  from (
    select
      min(date) as min_date,
      max(date) as max_date
    from {{ ref('int_prices__etf_daily') }}
  ),
  unnest(generate_date_array(min_date, max_date)) as date
),

symbols as (
  select symbol from {{ ref('dim_symbol') }}
)

select
  d.date,
  s.symbol
from dates d
cross join symbols s
```

This gives you a clean base grid where **every date & symbol** exists (you’ll left join real data on it).

---

## 9. Build the final feature table (`fct_etf_features_daily`)

Now the fun part: integrate all the marts.

`fct_etf_features_daily.sql`:

```sql
with spine as (
  select * from {{ ref('int_spine__date_symbol') }}
),

prices as (
  select
    date,
    symbol,
    close,
    adjusted_close,
    -- 1d return
    (adjusted_close / lag(adjusted_close) over (partition by symbol order by date) - 1) as ret_1d
    -- TODO: add 3m and 6m momentum etc...
  from {{ ref('int_prices__etf_daily') }}
),

yields as (
  select
    date,
    symbol,
    yield_12m_ttm,
    yield_3m_ann,
    yield_6m_ann
  from {{ ref('int_yield__etf_daily') }}
),

gov_bonds as (
  select * from {{ ref('int_yields__gov_bonds_daily') }}
),

-- Example: pivot some gov bonds into columns
gov_pivot as (
  select
    date,
    max(case when symbol = 'US10Y.GBOND' then yield end) as us10y,
    max(case when symbol = 'US5Y.GBOND'  then yield end) as us5y,
    max(case when symbol = 'DE10Y.GBOND' then yield end) as de10y,
    max(case when symbol = 'DE3Y.GBOND'  then yield end) as de3y
  from gov_bonds
  group by date
),

macro_us as (
  -- join your FRED YoY CPI + EFFR + SOFR here and forward-fill to daily
),

macro_eu as (
  -- join ECB CPI_EU_YOY + Euribor here and forward-fill to daily
),

indices as (
  -- pivot VIX, SX5E, GSPC, DXY from int_prices__indices_daily
)

select
  s.date,
  s.symbol,
  ds.etf_group,
  p.close,
  p.adjusted_close,
  p.ret_1d,
  y.yield_12m_ttm,
  y.yield_3m_ann,
  y.yield_6m_ann,
  g.us10y,
  g.us5y,
  g.de10y,
  g.de3y,
  -- spreads/slopes
  (g.us10y - g.us5y) as us_slope_10y5y,
  (g.de10y - g.de3y) as de_slope_10y3y,
  -- macro
  mu.cpi_us_yoy,
  me.cpi_eu_yoy,
  mu.effr,
  mu.sofr,
  -- indices like VIX, SX5E, etc.
  idx.vix,
  idx.sx5e,
  idx.dxy
from spine s
left join {{ ref('dim_symbol') }} ds
  on s.symbol = ds.symbol
left join prices      p on s.date = p.date and s.symbol = p.symbol
left join yields      y on s.date = y.date and s.symbol = y.symbol
left join gov_pivot   g on s.date = g.date
left join macro_us    mu on s.date = mu.date
left join macro_eu    me on s.date = me.date
left join indices     idx on s.date = idx.date
```

You’ll incrementally add:

- momentum features (3m/6m/12m returns) using window functions in the `prices` CTE  
- more spreads (like `yield_12m_ttm - us10y` for US credit ETFs)  
- category flags (`etf_group` one-hot) etc.  

This final model is what your ML pipeline will read.
