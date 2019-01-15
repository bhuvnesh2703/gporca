select a.cob_date ,sum(coalesce(a.USD_DELTA,0)) as USD_DELTA ,sum(coalesce(a.USD_EQ_PARTIAL_KAPPA,0)) as USD_EQ_KAPPA ,sum(coalesce(a. USD_IR_UNIFIED_PV01,0)) as USD_IR_UNIFIED_PV01 ,sum(coalesce(a. USD_PV01SPRD,0)) as USD_PV01SPRD ,sum(coalesce(a.USD_PV10_Bench,0)+coalesce(a.USD_Credit_PV10PCT,0)) as USD_CR_PV10  From cdwuser.U_DM_EQ a where (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-27') and a.CCC_PL_REPORTING_REGION = 'EMEA' AND a.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION'  AND  a.CCC_BANKING_TRADING = 'BANKING'   Group by a.cob_date