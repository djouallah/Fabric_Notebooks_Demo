-- materialized: (aemo,summary,ignore)
CREATE TEMP TABLE IF NOT EXISTS summary_temp(
  date DATE,
  "time" SMALLINT,
  cutoff TIMESTAMP WITH TIME ZONE,
  DUID VARCHAR,
  mw DECIMAL(18,4),
  price DECIMAL(18,4)
);
select * from summary_temp