SELECT     a.COB_DATE,     a.CURRENCY_OF_MEASURE,     CASE          WHEN a.TERM_OF_MEASURE <= 365 THEN '0-1y'         WHEN a.TERM_OF_MEASURE > 365 AND a.TERM_OF_MEASURE <= 1825 THEN '1-5y'         WHEN a.TERM_OF_MEASURE > 1825 AND a.TERM_OF_MEASURE <= 3650 THEN '5-10y'         WHEN a.TERM_OF_MEASURE > 3650 AND a.TERM_OF_MEASURE <= 5475 THEN   '10-15y'     ELSE '15+y' END AS TERM_NEW_GROUP,     SUM (CAST(a.USD_IR_UNIFIED_PV01 AS DOUBLE PRECISION)) AS usd_ir_unified_pv01 FROM cdwuser.U_IR_MSR_INTRPLT a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-27') and A.CCC_PL_REPORTING_REGION in ('ASIA PACIFIC') AND      a.CCC_DIVISION IN ('FIXED INCOME DIVISION') AND     /*a.CCC_BUSINESS_AREA NOT IN ('COMMODITIES') AND*/       CCC_BANKING_TRADING='TRADING' AND     (a.vertical_system LIKE ('FXDDI%') or a.vertical_system LIKE ('FXOPT%')) AND     a.CURRENCY_OF_MEASURE<>'USD' GROUP BY     a.COB_DATE,     a.CURRENCY_OF_MEASURE,     CASE          WHEN a.TERM_OF_MEASURE <= 365 THEN '0-1y'         WHEN a.TERM_OF_MEASURE > 365 AND a.TERM_OF_MEASURE <= 1825 THEN '1-5y'         WHEN a.TERM_OF_MEASURE > 1825 AND a.TERM_OF_MEASURE <= 3650 THEN '5-10y'         WHEN a.TERM_OF_MEASURE > 3650 AND a.TERM_OF_MEASURE <= 5475 THEN   '10-15y'     ELSE '15+y' END