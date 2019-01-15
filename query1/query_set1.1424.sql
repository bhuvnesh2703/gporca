SELECT 'EMEA' as CUT, a.COB_DATE, sum(a.USD_DELTA)/1000 as USD_DELTA FROM CDWUSER.U_DM_EQ a WHERE a.COB_DATE >= '2016-11-01' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BANKING_TRADING = 'TRADING' AND a.CCC_PL_REPORTING_REGION = 'EMEA' GROUP BY a.COB_DATE UNION ALL SELECT 'UK GROUP' as CUT, a.COB_DATE, sum(a.USD_DELTA)/1000 as USD_DELTA FROM CDWUSER.U_DM_EQ a WHERE a.COB_DATE >= '2016-11-01' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BANKING_TRADING = 'TRADING' AND a.IS_UK_GROUP = 'Y' GROUP BY a.COB_DATE UNION ALL SELECT 'GLOBAL' as CUT, a.COB_DATE, sum(a.USD_DELTA)/1000 as USD_DELTA FROM CDWUSER.U_DM_EQ a WHERE a.COB_DATE >= '2016-11-01' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BANKING_TRADING = 'TRADING' GROUP BY a.COB_DATE