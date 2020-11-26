CREATE OR REPLACE TABLE `your-project.skating.f_perf` (
perf_id             INT64    NOT NULL,
event_id            INT64    NOT NULL,
event_perf_seq_nbr  INT64    NOT NULL,
skater1_id          INT64    NOT NULL,
skater2_id          INT64,
event_perf_rank_nbr INT64,
component_score     FLOAT64,
element_score       FLOAT64,
deductions          FLOAT64);
