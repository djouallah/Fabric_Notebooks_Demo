-- materialized: (calendar,ignore)
SELECT cast(unnest(generate_series(cast ('2018-04-01' as date), cast('2026-12-31' as date), interval 1 day)) as date) as date,
            EXTRACT(year from date) as year,
            EXTRACT(month from date) as month