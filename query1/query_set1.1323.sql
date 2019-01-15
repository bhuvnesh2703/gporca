select cob_date, a.CCC_TAPS_COMPANY, a.TRADING_DESK, sum(abs(a.DELTA)) as GNURAMV from ( Select COB_DATE, d.CCC_TAPS_COMPANY ,CCC_PRODUCT_LINE as TRADING_DESK , d.issue_id_decomp , sum(d.USD_EQ_DELTA_DECOMP) as DELTA from cdwuser.U_DECOMP_MSR d where d.COB_DATE in ('2018-02-28','2018-01-31') and d.DIVISION = 'IED' and d.SILO_SRC = 'IED' and d.CCC_TAPS_COMPANY in ('0302','0342') and d.CCC_BANKING_TRADING <> 'BANKING' group by cob_date, CCC_TAPS_COMPANY ,CCC_PRODUCT_LINE , d.issue_id_decomp) a group by a.CCC_TAPS_COMPANY, a.COB_DATE, a.TRADING_DESK