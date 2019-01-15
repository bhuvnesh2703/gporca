SELECT      a.cob_date AS VARDATE,      a.CURRENCY_OF_MEASURE,     CASE         WHEN CCC_BANKING_TRADING='TRADING' THEN 'TRADING'     ELSE 'BANKING' END AS BT_FLAG,     Sum(usd_fx) AS USD_FX FROM   cdwuser.u_fx_msr a  WHERE     (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-27') and A.CCC_PL_REPORTING_REGION in ('ASIA PACIFIC') AND      (A.CCC_DIVISION IN ('FIXED INCOME DIVISION') OR     (A.CCC_DIVISION IN ('INSTITUTIONAL EQUITY DIVISION') AND a.CCC_BUSINESS_AREA NOT IN ('INTERNATIONAL WEALTH MGMT')) OR     A.CCC_DIVISION IN ('BANK RESOURCE MANAGEMENT') OR     A.CCC_DIVISION IN ('INSTITUTIONAL SECURITIES OTHER')) AND     A.USD_FX IS NOT NULL group by     cob_date,     a.CURRENCY_OF_MEASURE,     CASE         WHEN CCC_BANKING_TRADING='TRADING' THEN 'TRADING'     ELSE 'BANKING' END