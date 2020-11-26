CREATE OR REPLACE TABLE `your-project.skating.f_perf_aspect` (
perf_aspect_id      INT64    NOT NULL,
perf_id             INT64    NOT NULL,
perf_aspect_seq_nbr INT64,
aspect_id           INT64    NOT NULL,
base_difficulty     FLOAT64,
total_score         FLOAT64  NOT NULL);
