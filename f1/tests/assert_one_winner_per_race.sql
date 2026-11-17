-- Every race that has results must have exactly one classified winner.
-- Returns the offending races; the test passes when nothing is returned.

select
    race_key,
    count_if(is_winner) as winners
from {{ ref('fct_race_results') }}
group by race_key
having count_if(is_winner) != 1
