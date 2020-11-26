CREATE OR REPLACE VIEW `your-project.skating.s_person_v` AS
with 
raw_people as (
select distinct nationality, gender, nm
from
   (select nationality, cast(null as string) as gender, judge_nm as nm 
    from `your-project.skating.s_judges` 
	
    union all
    select nationality, 'F' as gender, skater_nm as nm 
    from `your-project.skating.s_performances` 
	where program_event_nm like '%Ladies%' 
	
    union all
    select nationality, 'M' as gender, skater_nm as nm 
    from `your-project.skating.s_performances` 
	where program_event_nm like '%Men%'
	 
	union all
    select nationality, 'F' as gender, trim(substr(skater_nm,1,strpos(skater_nm,'/')-1)) as nm
    from `your-project.skating.s_performances` 
	where skater_nm like '%/%' 
	
	union all
    select nationality, 'M' as gender, trim(substr(skater_nm,strpos(skater_nm,'/')+1)) as nm 
    from `your-project.skating.s_performances` 
	where skater_nm like '%/%')),
    
people as (
select 
  a.*,
  func.initcap(
      substr(nm,1,strpos(regexp_replace(upper(substr(nm,1,strpos(nm,' ')-1)) || substr(nm,strpos(nm,' ')),'[a-z]','x'),'x')-3)) as surname,
  func.initcap(trim(
      replace(nm,substr(nm,1,strpos(regexp_replace(upper(substr(nm,1,strpos(nm,' ')-1)) || substr(nm,strpos(nm,' ')),'[a-z]','x'),'x')-3),''))) as forename
from
  raw_people a)

select 
  row_number() over (order by surname, forename, nationality, gender) as person_id,
  forename, surname, gender, nationality, nm as raw_nm
from 
  people
;
