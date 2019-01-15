SELECT v.COB_DATE, sum(v.SLIDE_EQ_MIN_30_USD) as D30raw FROM CDWUSER.U_EQ_MSR v WHERE v.COB_DATE in ('2018-02-28','2018-02-21','2018-02-14','2018-02-07','2018-02-07','2018-01-31','2018-01-24','2018-01-17','2018-01-10','2018-01-03') AND v.SILO_SRC = 'IED' AND v.DIVISION = 'IED' AND v.CCC_BANKING_TRADING ='TRADING' AND v.CCC_PL_REPORTING_REGION = 'EMEA' GROUP BY v.COB_DATE