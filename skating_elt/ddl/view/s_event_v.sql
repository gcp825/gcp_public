CREATE OR REPLACE VIEW `your-project.skating.s_event_v` AS
select
  row_number() over (order by competition_nm, program_nm, element_nm) as event_id, 
  a.*
from (
  select
    competition_nm,
    rtrim(trim(replace(program_event_nm, pe_array[ordinal(len-1)] || ' ' || pe_array[ordinal(len)], '')),' -') as program_nm,
    pe_array[ordinal(len-1)] || ' ' || pe_array[ordinal(len)] as element_nm,
    entries_ct,
    program_event_nm as raw_event_nm
  from (
    select
      competition_nm,
      program_event_nm,
      split(program_event_nm,' ') as pe_array,    
      array_length(split(program_event_nm,' ')) as len, 
      count(*) as entries_ct
    from
      `your-project.skating.s_performances`
    group by
      1,2)) a
;
