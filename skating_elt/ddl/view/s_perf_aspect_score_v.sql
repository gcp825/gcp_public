CREATE OR REPLACE VIEW `your-project.skating.s_perf_aspect_score_v` AS
with 
scores as (
select
  b.perf_aspect_id,
  f.person_id as judge_id,
  a.score
from
  `your-project.skating.s_judge_scores` a
left outer join
  `your-project.skating.s_perf_aspect_v` b on (a.judged_aspect_id = b.raw_judged_aspect_id)
left outer join
  `your-project.skating.s_perf_v` c on (b.perf_id = c.perf_id)
left outer join
  `your-project.skating.s_event_v` d on (c.event_id = d.event_id)
left outer join
  `your-project.skating.s_judges` e on (d.competition_nm = e.competition_nm
                                    and d.raw_event_nm = e.program_event_nm
                                    and a.clean_role = e.clean_role)
left outer join
  `your-project.skating.s_person_v` f on (e.judge_nm = f.raw_nm)
)

select 
  row_number() over (order by perf_aspect_id, judge_id) as perf_aspect_score_id, 
  a.*
from
  scores a
;
