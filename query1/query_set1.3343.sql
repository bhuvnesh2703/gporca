SELECT X.COB_DATE, X.FUNDING_FLAG, X.BOOK, X.CURRENCY_OF_MEASURE, case when X.EXPIRATION_DATE <= X.COB_DATE + 1 then '01. 1D' when X.EXPIRATION_DATE <= X.COB_DATE + 7 then '02. 1W' when X.EXPIRATION_DATE <= X.COB_DATE + 14 then '03. 2W' when X.EXPIRATION_DATE <= X.COB_DATE + 21 then '04. 3W' when X.EXPIRATION_DATE <= X.COB_DATE + 28 then '05. 4W' when X.EXPIRATION_DATE <= X.COB_DATE + 35 then '06. 5W' when X.EXPIRATION_DATE <= X.COB_DATE + 42 then '07. 6W' when X.EXPIRATION_DATE <= X.COB_DATE + 49 then '08. 7W' when X.EXPIRATION_DATE <= X.COB_DATE + 56 then '09. 8W' when X.EXPIRATION_DATE <= X.COB_DATE + 63 then '10. 9W' when X.EXPIRATION_DATE <= X.COB_DATE + 70 then '11. 10W' when X.EXPIRATION_DATE <= X.COB_DATE + 77 then '12. 11W' when X.EXPIRATION_DATE <= X.COB_DATE + 84 then '13. 12W' when X.EXPIRATION_DATE <= X.COB_DATE + 91 then '14. 13W' when X.EXPIRATION_DATE <= X.COB_DATE + 98 then '15. 14W' when X.EXPIRATION_DATE <= X.COB_DATE + 105 then '16. 15W' when X.EXPIRATION_DATE <= X.COB_DATE + 112 then '17. 16W' when X.EXPIRATION_DATE <= X.COB_DATE + 119 then '18. 17W' when X.EXPIRATION_DATE <= X.COB_DATE + 126 then '19. 18W' when X.EXPIRATION_DATE <= X.COB_DATE + 133 then '20. 19W' when X.EXPIRATION_DATE <= X.COB_DATE + 140 then '21. 20W' when X.EXPIRATION_DATE <= X.COB_DATE + 180 then '22. 6M' when X.EXPIRATION_DATE <= X.COB_DATE + 210 then '23. 7M' when X.EXPIRATION_DATE <= X.COB_DATE + 240 then '24. 8M' when X.EXPIRATION_DATE <= X.COB_DATE + 270 then '25. 9M' when X.EXPIRATION_DATE <= X.COB_DATE + 300 then '26. 10M' when X.EXPIRATION_DATE <= X.COB_DATE + 330 then '27. 11M' when X.EXPIRATION_DATE <= X.COB_DATE + 365 then '28. 1Y' else '29. +1Y' end AS TERM, case when X.EXPIRATION_DATE < X.COB_DATE + 30 then 'Within 30d' when X.EXPIRATION_DATE< X.COB_DATE + 365 then 'Within 365d' else 'Outside 365d' end AS TERM2, X.EXPIRATION_DATE, X.USD_NOTIONAL FROM ( SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE, SUM (-f.USD_NOTIONAL::numeric(15,5)) AS USD_NOTIONAL FROM CDWUSER.U_OT_MSR F WHERE f.cob_date in ('2018-02-28', '2018-01-31')and f.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' AND f.VERTICAL_SYSTEM LIKE '%FXDDI%' AND f.BOOK not in ('CT0302XE', 'CT0101XE') AND f.CCC_BUSINESS_AREA IN ('TSY DEBT') AND f.CCC_PRODUCT_LINE IN ('S-TERM DEBT', 'FX AND OTHER FUNDING') AND f.CCC_STRATEGY IN ('FX SWAPS') AND f.USD_NOTIONAL IS NOT NULL AND f.USD_NOTIONAL <> 0 AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE UNION ALL SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE, SUM (-f.USD_NOTIONAL::numeric(15,5)) AS USD_NOTIONAL FROM CDWUSER.U_OT_MSR F WHERE f.cob_date in ('2018-02-28', '2018-01-31') AND f.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' AND f.CCC_BUSINESS_AREA = 'LIQUIDITY RESERVE1' AND f.ACCOUNT = '075006NK8' AND f.USD_NOTIONAL IS NOT NULL AND f.USD_NOTIONAL <> 0 AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE UNION ALL SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE, SUM (-f.USD_MARKET_VALUE::numeric(15,5)) AS USD_NOTIONAL FROM CDWUSER.U_OT_MSR F WHERE f.cob_date in ('2018-02-28', '2018-01-31') and f.BOOK IN ('KRSWP') AND f.USD_MARKET_VALUE IS NOT NULL AND f.USD_MARKET_VALUE <> 0 AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE UNION ALL SELECT COB_DATE, Case when XCCY_MAPPING = 'XCCY SWAP' then 'XCCY' else XCCY_MAPPING end as FUNDING_FLAG, BOOK, CURRENCY_OF_MEASURE, EXPIRATION_DATE, SUM(-usd_notional::numeric(15,5) / 1000) AS USD_NOTIONAL FROM cdwuser.U_DM_TREASURY a WHERE cob_date in ('2018-02-28', '2018-01-31') AND A.DATASET_TYPE = 'LRV' AND BOOK NOT IN ('TLNFX', 'TZUFX', 'TSTJY', 'TSGFX', 'THKFX') AND A.EXPIRATION_DATE > A.COB_DATE AND A.XCCY_MAPPING = 'XCCY SWAP' GROUP BY COB_DATE, Case when XCCY_MAPPING = 'XCCY SWAP' then 'XCCY' else XCCY_MAPPING end, BOOK, CURRENCY_OF_MEASURE, EXPIRATION_DATE ) X