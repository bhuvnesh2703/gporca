SELECT cob_date, a.ccc_division, SUM (usd_fx) AS FX FROM cdwuser.U_DM_FX a WHERE COB_DATE >= '2014-12-31' and currency_of_measure in ('ZAR') AND USD_FX IS NOT NULL AND USD_FX <> 0 AND a.ccc_business_area NOT IN ('CPM', 'LENDING', 'OTC CLIENT CLEARING') AND a.ccc_division IN ('FIXED INCOME DIVISION', 'INSTITUTIONAL EQUITY DIVISION') AND a.CCC_BANKING_TRADING_LOCALREG = 'TRADING' GROUP BY a.COB_DATE, a.ccc_division