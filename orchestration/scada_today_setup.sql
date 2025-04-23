-- materialized: (raw,scada_today,ignore)
CREATE TEMP TABLE IF NOT EXISTS scada_today_temp(
  DUID VARCHAR,
  INITIALMW DOUBLE,
  INTERVENTION DOUBLE,
  SETTLEMENTDATE TIMESTAMP WITH TIME ZONE,
  date DATE,
  file VARCHAR,
  PRIORITY INTEGER,
  "YEAR" BIGINT
);
from scada_today_temp