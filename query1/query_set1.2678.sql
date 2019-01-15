SELECT a.COB_DATE, a.CCC_RISK_MANAGER_LOGIN, a.CCC_BUSINESS_AREA, a.CCC_STRATEGY, SUM(coalesce(a.USD_EQ_PRTL_DIV_RISK_ANN,0)+ coalesce(a.USD_EQ_PRTL_DIV_RISK_PRO,0)) AS DIV FROM CDWUSER.U_EQ_MSR a WHERE a.COB_DATE IN('2018-02-28', '2018-02-27') AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_PL_REPORTING_REGION = 'AMERICAS' GROUP BY a.COB_DATE, a.CCC_RISK_MANAGER_LOGIN, a.CCC_BUSINESS_AREA, a.CCC_STRATEGY