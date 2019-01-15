SELECT a.COB_DATE, a.CCC_PL_REPORTING_REGION, sum(a.USD_DELTA) as USD_EQ_DELTA FROM CDWUSER.U_DM_EQ a WHERE a.COB_DATE in ('2018-02-28','2018-02-27') AND a.CCC_BANKING_TRADING = 'BANKING' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.SILO_SRC = 'IED' GROUP BY a.COB_DATE, a.CCC_PL_REPORTING_REGION