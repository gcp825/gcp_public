CREATE OR REPLACE VIEW `your-project.skating.s_perf_aspect_v` AS
with 
perf_aspects as (
select
  b.perf_id,
  cast(a.perf_aspect_seq_nbr as int64) as perf_aspect_seq_nbr,
  c.aspect_id,
  a.base_difficulty,
  a.total_score,
  a.judged_aspect_id as raw_judged_aspect_id
from
  `your-project.skating.s_judged_aspects` a
left outer join
  `your-project.skating.s_perf_v` b on (a.performance_id = b.raw_performance_id)
left outer join
  `your-project.skating.s_aspect_v` c on (a.aspect_desc = c.aspect_desc and upper(substr(a.aspect_type_nm,1,1)) = c.aspect_type_cd)
)

select
  row_number() over (order by perf_id, coalesce(perf_aspect_seq_nbr,0), aspect_id) as perf_aspect_id, 
  a.*
from
  perf_aspects a
;
