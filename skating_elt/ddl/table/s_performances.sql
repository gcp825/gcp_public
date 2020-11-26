CREATE OR REPLACE TABLE `your-project.skating.s_performances` (
performance_id      STRING  NOT NULL,
competition_nm      STRING  NOT NULL,
program_event_nm    STRING  NOT NULL,
skater_nm           STRING  NOT NULL,
nationality         STRING  NOT NULL,
event_perf_rank_nbr INT64   NOT NULL,
event_perf_seq_nbr  INT64   NOT NULL,
segment_score       FLOAT64 NOT NULL,
element_score       FLOAT64 NOT NULL,
component_score     FLOAT64 NOT NULL,
deductions          FLOAT64 NOT NULL);
