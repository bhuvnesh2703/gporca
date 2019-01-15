SELECT SUM (a.USD_FX) AS usd_fx, a.ccc_business_area, a.CCC_BANKING_TRADING_LOCALREG, a.CCC_PL_REPORTING_REGION, a.COB_DATE, CASE WHEN a.CURRENCY_OF_MEASURE IN ('GBP', 'JPY', 'EUR', 'AUD', 'CAD', 'CHF', 'DKK', 'NOK', 'NZD', 'SEK') THEN 'G10' WHEN a. CURRENCY_OF_MEASURE IN ('USD', 'UBD') THEN 'US' WHEN a.CURRENCY_OF_MEASURE IN ('AED', 'HKD', 'SAR', 'OMR', 'KWD', 'BHD') THEN 'PEGGED' ELSE 'EM' END AS currency_group FROM cdwuser.U_DM_FX a WHERE COB_DATE >= '2017-08-28' AND CCC_PL_REPORTING_REGION IN ('EMEA') AND a.CCC_BANKING_TRADING_LOCALREG IN ('TRADING') AND ccc_business_area IN ('FXEM MACRO TRADING', 'EM CREDIT TRADING') AND USD_FX IS NOT NULL AND USD_FX <> 0 GROUP BY a.ccc_business_area, a.CCC_BANKING_TRADING_LOCALREG, CCC_PL_REPORTING_REGION, a.COB_DATE, CASE WHEN a.CURRENCY_OF_MEASURE IN ('GBP', 'JPY', 'EUR', 'AUD', 'CAD', 'CHF', 'DKK', 'NOK', 'NZD', 'SEK') THEN 'G10' WHEN a. CURRENCY_OF_MEASURE IN ('USD', 'UBD') THEN 'US' WHEN a.CURRENCY_OF_MEASURE IN ('AED', 'HKD', 'SAR', 'OMR', 'KWD', 'BHD') THEN 'PEGGED' ELSE 'EM' END