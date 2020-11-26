# Retained as an example of UDF syntax / UNNEST -  but deprecated since Oct 2020 as INITCAP
#   now exists as an out-of-the-box native BigQuery SQL function (finally!)

CREATE OR REPLACE FUNCTION `your-project.func.initcap` (str STRING) AS (
( 
  SELECT 
    CASE 
      WHEN str = '' THEN ''
      ELSE STRING_AGG(UPPER(SUBSTR(words,1,1)) || LOWER(SUBSTR(words,2)), '' ORDER BY pos)
    END
  FROM 
    UNNEST(REGEXP_EXTRACT_ALL(str, r" +|-+|'+|.[^ '-]*")) AS words
  WITH 
    OFFSET AS pos
));
