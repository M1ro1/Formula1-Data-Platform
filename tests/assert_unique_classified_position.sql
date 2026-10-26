select race_key, finish_position
from ref{{fct_race_results}}
where is_classified
group by race_key, finish_position
having count(*) > 1