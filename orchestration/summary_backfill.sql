
-- materialized: (summary,delta,overwrite)
       select
        s.date,
        cast(strftime(s.SETTLEMENTDATE, '%H%M') AS INT16)                       as time ,
        (select max(cast(settlementdate as TIMESTAMPTZ) ) from scada)  as cutoff ,
        s.DUID,
        cast(max(s.INITIALMW) AS DECIMAL(18, 4))                                as mw,
        cast(max(p.RRP) AS DECIMAL(18, 4))                                      as price
      from  scada   s
            LEFT JOIN duid d    ON s.DUID = d.DUID
            LEFT JOIN price   p ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
      where
        s.INTERVENTION = 0
        and INITIALMW <> 0 
        and p.INTERVENTION = 0  
      group by all
      order by  s.date, s.DUID, time,price;