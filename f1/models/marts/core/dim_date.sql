with spine as (
    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="cast('1950-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
)

select
    date_day as full_date,        -- 2023-01-01
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,         -- 20230101

    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    monthname(date_day) as month_name,
    day(date_day) as day_of_month,

    weekiso(date_day) as iso_week,          -- 1-53
    dayofweekiso(date_day) as iso_day_of_week,   -- 1 = Monday
    dayname(date_day) as day_name,
    dayofweekiso(date_day) >= 6 as is_weekend         -- Saturday or Sunday

from spine
