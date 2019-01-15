SELECT SUM (a.USD_IR_UNIFIED_PV01) AS PV01, a.ccc_business_area, a.COB_DATE, a.CCC_PL_REPORTING_REGION, CASE WHEN a.CURRENCY_OF_MEASURE IN ('EUR') THEN 'EUR' WHEN a.CURRENCY_OF_MEASURE IN ('USD', 'UBD') THEN 'USD' WHEN a. CURRENCY_OF_MEASURE IN ('GBP') THEN 'GBP' WHEN a.CURRENCY_OF_MEASURE IN ('NOK', 'SEK', 'CAD', 'DKK', 'CHF', 'AUD', 'NZD', 'JPY') THEN 'OTH G10' ELSE 'EM' END AS GROUP_CCY FROM cdwuser.U_DM_IR a WHERE COB_DATE >= '2017-08-28' AND ccc_business_area IN ('FXEM MACRO TRADING', 'EM CREDIT TRADING') AND CCC_PL_REPORTING_REGION IN ('EMEA') AND USD_IR_UNIFIED_PV01 IS NOT NULL AND USD_IR_UNIFIED_PV01 <> 0 GROUP BY a.ccc_business_area, a.COB_DATE, a.CCC_PL_REPORTING_REGION, CASE WHEN a.CURRENCY_OF_MEASURE IN ('EUR') THEN 'EUR' WHEN a.CURRENCY_OF_MEASURE IN ('USD', 'UBD') THEN 'USD' WHEN a. CURRENCY_OF_MEASURE IN ('GBP') THEN 'GBP' WHEN a.CURRENCY_OF_MEASURE IN ('NOK', 'SEK', 'CAD', 'DKK', 'CHF', 'AUD', 'NZD', 'JPY') THEN 'OTH G10' ELSE 'EM' END