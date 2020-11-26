CREATE OR REPLACE VIEW `your-project.skating.s_perf_v` AS
with perfs as (
select
  b.event_id,
  a.event_perf_seq_nbr,
  c.person_id as skater1_id,
  d.person_id as skater2_id,
  a.event_perf_rank_nbr,
  a.component_score,
  a.element_score,
  a.deductions,
  a.performance_id as raw_performance_id
from 
  `your-project.skating.s_performances` a
left outer join
  `your-project.skating.s_event_v` b on (a.competition_nm = b.competition_nm
                                     and a.program_event_nm = b.raw_event_nm)
left outer join
  `your-project.skating.s_person_v` c on (trim(substr(a.skater_nm,1,strpos(a.skater_nm || '/','/')-1)) = c.raw_nm)
left outer join
  `your-project.skating.s_person_v` d on (trim(trim(substr(a.skater_nm,strpos(a.skater_nm || '/','/')),'/')) = d.raw_nm)
)

select
  row_number() over (order by event_id, event_perf_seq_nbr) as perf_id, 
  a.*
from
  perfs a
;
