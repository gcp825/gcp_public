CREATE OR REPLACE VIEW `your-project.skating.s_aspect_v` AS
select distinct
  dense_rank() over (order by aspect_type_nm, aspect_desc) as aspect_id,
  upper(substr(aspect_type_nm,1,1)) as aspect_type_cd,
  aspect_desc
from
  `your-project.skating.s_judged_aspects`
;
