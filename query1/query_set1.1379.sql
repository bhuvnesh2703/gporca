SELECT a.COB_DATE, sum(a.USD_EQ_KAPPA) as USD_EQ_KAPPA FROM CDWUSER.U_DM_EQ a WHERE a.COB_DATE >= '2017-09-27' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BANKING_TRADING = 'TRADING' AND (a.EXECUTIVE_MODEL LIKE '%VAR-SWAP%' OR a.EXECUTIVE_MODEL LIKE '%VARIANCESWAP%') GROUP BY a.COB_DATE