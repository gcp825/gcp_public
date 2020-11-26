CREATE OR REPLACE TABLE `your-project.skating.s_judged_aspects` (
judged_aspect_id    STRING   NOT NULL,
performance_id      STRING   NOT NULL,
aspect_type_nm      STRING   NOT NULL,
perf_aspect_seq_nbr FLOAT64,
aspect_desc         STRING   NOT NULL,
info_flag           STRING,
credit_flag         STRING,
base_difficulty     FLOAT64,
factor              FLOAT64,
goe                 FLOAT64,
ref                 STRING,
total_score         FLOAT64  NOT NULL);
