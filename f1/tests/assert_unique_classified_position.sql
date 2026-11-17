-- Two classified cars can't share a finishing position in the same race.
-- Returns the duplicates; the test passes when nothing is returned.

select
    race_key,
    finish_position,
    count(*) as cars
from {{ ref('fct_race_results') }}
where is_classified
group by race_key, finish_position
having count(*) > 1
