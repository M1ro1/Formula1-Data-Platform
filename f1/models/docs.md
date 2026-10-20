{% docs __overview__ %}

# F1 Data Warehouse

End-to-end ELT project: Formula 1 data from the public **Jolpica-F1 API** is ingested by **Airflow**,
loaded into **Snowflake**, and modeled with **dbt** into a star schema.

## Layers

| Layer | Schema | What lives here |
|---|---|---|
| Sources | `RAW` | Raw API pages as `VARIANT`, one table per entity, plus load metadata |
| Staging | `STAGING` | `stg_f1__*`: JSON flattened, typed and renamed, 1:1 with the source entities |
| Intermediate | `INTERMEDIATE` | Reusable preparation logic (only where it is really needed) |
| Marts: core | `MARTS` | `dim_*` dimensions and `fct_*` facts (star schema) |
| Marts: analytics | `MARTS` | `mart_*` aggregates built for dashboards |

## How to read the lineage graph

`source` (RAW) -> `stg_f1__*` -> `dim_*` / `fct_*` -> `mart_*`.
Facts reference dimensions through surrogate keys (`*_key`), which are hashes of the natural ids,
so a fact never needs a join just to get a foreign key.

## Things worth knowing

- **Points are race points only.** Sprint-race points are not loaded, so season totals can differ
  from the official championship in sprint seasons.
- `dim_driver_team_history` is an SCD Type 2 built from race results (gaps and islands),
  so the full history is available without waiting for snapshot runs.
- `fct_lap_times` is incremental (merge); everything else is rebuilt on each run.

*Data provided by the Jolpica-F1 API. This project is not affiliated with Formula 1.*

{% enddocs %}


{% docs race_key %}Surrogate key of the race (hash of `race_id`). Joins to `dim_race.race_key`.{% enddocs %}

{% docs driver_key %}Surrogate key of the driver (hash of `driver_id`). Joins to `dim_driver.driver_key`.{% enddocs %}

{% docs constructor_key %}Surrogate key of the constructor/team (hash of `constructor_id`). Joins to `dim_constructor.constructor_key`.{% enddocs %}

{% docs circuit_key %}Surrogate key of the circuit (hash of `circuit_id`). Joins to `dim_circuit.circuit_key`.{% enddocs %}

{% docs race_date_key %}Race date as an integer `YYYYMMDD`. Joins to `dim_date.date_key`.{% enddocs %}

{% docs season %}Championship season (year).{% enddocs %}

{% docs round %}Round number within the season (1 = first race).{% enddocs %}

{% docs is_classified %}True when the driver was classified in the race result. The source marks classified drivers with a numeric position; letters (R, D, W, ...) mean not classified.{% enddocs %}

{% docs race_points_note %}Race points only. Sprint-race points are not loaded, so totals can differ from the official championship in sprint seasons.{% enddocs %}
