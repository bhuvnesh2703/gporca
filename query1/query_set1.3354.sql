SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE, SUM (f.USD_NOTIONAL::numeric(15,5)) AS USD_NOTIONAL FROM cdwuser.U_OT_MSR F WHERE F.COB_DATE IN ('2018-01-31', '2018-02-28') AND f.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' AND f.VERTICAL_SYSTEM LIKE '%FXDDI%' AND f.BOOK not in ('CT0302XE', 'CT0101XE') AND f.CCC_BUSINESS_AREA IN ('TSY DEBT') AND f.CCC_PRODUCT_LINE IN ('S-TERM DEBT', 'FX AND OTHER FUNDING') AND f.CCC_STRATEGY IN ('FX SWAPS') AND f.USD_NOTIONAL IS NOT NULL AND f.USD_NOTIONAL <> 0 AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE UNION ALL SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE, case when f.USD_MARKET_VALUE >=0 then sum(f.USD_MARKET_VALUE::numeric(15,5)) else -sum(f.USD_MARKET_VALUE::numeric(15,5)) end as USD_NOTIONAL FROM cdwuser.U_OT_MSR F WHERE F.COB_DATE IN ('2018-01-31', '2018-02-28') AND f.BOOK IN ('TSGFX', 'THKFX') AND F.USD_MARKET_VALUE IS NOT NULL AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE, F.BOOK, f.CURRENCY_OF_MEASURE, F.USD_MARKET_VALUE, f.EXPIRATION_DATE, f.long_short UNION ALL SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE, SUM (f.USD_MARKET_VALUE::numeric(15,5)) AS USD_NOTIONAL FROM cdwuser.U_OT_MSR F WHERE F.COB_DATE IN ('2018-01-31', '2018-02-28') AND f.BOOK IN ('KRSWP') AND f.USD_MARKET_VALUE IS NOT NULL AND f.USD_MARKET_VALUE <> 0 AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE UNION ALL SELECT COB_DATE, 'CTA' as FUNDING_FLAG, BOOK, CURRENCY_OF_MEASURE, EXPIRATION_DATE, SUM (USD_NOTIONAL::numeric(15,5)) AS USD_NOTIONAL FROM cdwuser.U_OT_MSR WHERE COB_DATE IN ('2018-01-31', '2018-02-28') AND CCC_DIVISION = 'TREASURY CAPITAL MARKETS' AND BOOK IN ('CT0101FWD', 'CTFW', 'TYSJV', 'CT0302SPT', 'CT0101FWDP') AND USD_NOTIONAL IS NOT NULL AND COB_DATE < EXPIRATION_DATE GROUP BY COB_DATE, BOOK, CURRENCY_OF_MEASURE, EXPIRATION_DATE UNION ALL SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE, CASE WHEN F.CURRENCY_OF_MEASURE = 'USD' THEN SUM(F.USD_NOTIONAL::numeric(15,5)) *-1 ELSE SUM(F.USD_NOTIONAL::numeric(15,5)) END AS USD_NOTIONAL FROM cdwuser.U_OT_MSR F WHERE f. COB_DATE IN ('2018-01-31', '2018-02-28') AND f.BOOK IN ('TLNFX', 'TZUFX') AND f.USD_NOTIONAL IS NOT NULL AND f.USD_NOTIONAL<> 0 AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE, F.BOOK, f.CURRENCY_OF_MEASURE, f.EXPIRATION_DATE