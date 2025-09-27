        select
        s.DATE as date,
        cast(strftime(s.SETTLEMENTDATE, '%H%M') AS INT16)                       as time ,
        (select max(cast(settlementdate as TIMESTAMPTZ) ) from scada)           as cutoff ,
        s.DUID,
        max(s.INITIALMW)                                                        as mw,
        max(p.RRP)                                                              as price
      from  scada   s
            LEFT JOIN duid d    ON s.DUID = d.DUID
            LEFT JOIN price   p ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
      where
        s.INTERVENTION = 0
        and INITIALMW <> 0
        and p.INTERVENTION = 0
      group by all
      ORDER BY date, s.DUID, time, price;
