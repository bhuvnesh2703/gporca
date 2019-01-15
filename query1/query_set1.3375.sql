SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, SUM (f.USD_NOTIONAL::numeric(15,5)) AS USD_NOTIONAL FROM CDWUSER.U_OT_MSR F WHERE (F.COB_DATE >='2018-02-22' and F.COB_DATE <= '2018-02-28') AND f.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' AND f.VERTICAL_SYSTEM LIKE '%FXDDI%' AND f.BOOK not in ('CT0302XE', 'CT0101XE') AND f.CCC_BUSINESS_AREA IN ('TSY DEBT') AND f.CCC_PRODUCT_LINE IN ('S-TERM DEBT', 'FX AND OTHER FUNDING') AND f.CURRENCY_OF_MEASURE<>'USD' AND f.USD_NOTIONAL IS NOT NULL AND f.USD_NOTIONAL <> 0 AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE UNION ALL SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, case when f.USD_MARKET_VALUE >=0 then sum(f.USD_MARKET_VALUE) else -sum(f.USD_MARKET_VALUE::numeric(15,5)) end as USD_NOTIONAL FROM CDWUSER.U_OT_MSR F WHERE (F.COB_DATE >='2018-02-22' and F.COB_DATE <= '2018-02-28') AND f.BOOK IN ('TSGFX', 'THKFX') AND f.CURRENCY_OF_MEASURE<>'USD' AND F.USD_MARKET_VALUE IS NOT NULL AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE, f.USD_MARKET_VALUE UNION ALL SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, SUM (f.USD_MARKET_VALUE::numeric(15,5)) AS USD_NOTIONAL FROM CDWUSER.U_OT_MSR F WHERE (F.COB_DATE >='2018-02-22' and F.COB_DATE <= '2018-02-28') AND f.BOOK IN ('KRSWP') AND f.CURRENCY_OF_MEASURE<>'USD' AND f.USD_MARKET_VALUE IS NOT NULL AND f.USD_MARKET_VALUE <> 0 AND f.COB_DATE < f.EXPIRATION_DATE GROUP BY f.COB_DATE UNION ALL SELECT f.COB_DATE, 'Funding' as FUNDING_FLAG, CASE WHEN F.CURRENCY_OF_MEASURE = 'USD' THEN SUM(F.USD_NOTIONAL) *-1 ELSE SUM(F.USD_NOTIONAL::numeric(15,5)) END AS USD_NOTIONAL FROM CDWUSER.U_OT_MSR F WHERE (F.COB_DATE >='2018-02-22' and F.COB_DATE <= '2018-02-28') AND f.BOOK IN ('TLNFX', 'TZUFX') AND f.USD_NOTIONAL IS NOT NULL AND f.USD_NOTIONAL<> 0 AND f.COB_DATE < f.EXPIRATION_DATE AND F.CURRENCY_OF_MEASURE <> 'USD' GROUP BY f.COB_DATE, F.CURRENCY_OF_MEASURE