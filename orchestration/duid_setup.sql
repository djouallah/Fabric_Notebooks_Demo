-- materialized: (aemo,duid,ignore)
CREATE TEMP TABLE IF NOT EXISTS duid_temp(
  DUID VARCHAR,
  Region VARCHAR,
  FuelSourceDescriptor VARCHAR,
  Participant VARCHAR,
  State VARCHAR
);
select * from duid_temp