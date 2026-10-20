select race_key
from {{ref{'fct_race_results'}}}
group by race_key
having count(distinct winner_key) > 1