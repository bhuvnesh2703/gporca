SELECT cob_date, CCC_PL_REPORTING_REGION , sum(a.EQ_DELTA) as EQ_DELTA , sum(coalesce(a.SLIDE_EQ_MIN_1_USD, a.EQ_DELTA*-0.01, 0)) as eq_down_1 , sum(coalesce(a.SLIDE_EQ_MIN_02_USD, a.EQ_DELTA*-0.02, 0)) as eq_down_02 , sum(coalesce(a.SLIDE_EQ_MIN_05_USD, a.EQ_DELTA*-0.05, 0)) as eq_down_05 , sum(coalesce(a.SLIDE_EQ_MIN_10_USD, a.EQ_DELTA*-0.1, 0)) as eq_down_10 , sum(coalesce(a.SLIDE_EQ_MIN_20_USD, a.EQ_DELTA*-0.2, 0)) as eq_down_20 , sum(coalesce(a.SLIDE_EQ_MIN_30_USD, a.EQ_DELTA*-0.3, 0)) as eq_down_30 , sum(coalesce(a.SLIDE_EQ_MIN_50_USD, a.EQ_DELTA*-0.5, 0)) as eq_down_50 , sum(coalesce(a.SLIDE_EQ_PLS_1_USD, a.EQ_DELTA*0.01, 0)) as eq_up_1 , sum(coalesce(a.SLIDE_EQ_PLS_02_USD, a.EQ_DELTA*0.02, 0)) as eq_up_02 , sum(coalesce(a.SLIDE_EQ_PLS_05_USD, a.EQ_DELTA*0.05, 0)) as eq_up_05 , sum(coalesce(a.SLIDE_EQ_PLS_10_USD, a.EQ_DELTA*0.1, 0)) as eq_up_10 , sum(coalesce(a.SLIDE_EQ_PLS_20_USD, a.EQ_DELTA*0.2, 0)) as eq_up_20 , sum(coalesce(a.SLIDE_EQ_PLS_30_USD, a.EQ_DELTA*0.3, 0)) as eq_up_30 FROM ( SELECT cob_date , b.PROCESS_ID, b.POSITION_KEY , b.CCC_TAPS_COMPANY, ccc_division, CCC_PL_REPORTING_REGION , sum(coalesce(b.USD_DELTA, 0)) as EQ_DELTA , sum(b.SLIDE_EQ_MIN_50_USD) as SLIDE_EQ_MIN_50_USD , sum(b.SLIDE_EQ_MIN_30_USD) as SLIDE_EQ_MIN_30_USD , sum(b.SLIDE_EQ_MIN_20_USD) as SLIDE_EQ_MIN_20_USD , sum(b.SLIDE_EQ_MIN_10_USD) as SLIDE_EQ_MIN_10_USD , sum(b.SLIDE_EQ_MIN_05_USD) as SLIDE_EQ_MIN_05_USD , sum(b.SLIDE_EQ_MIN_1_USD) as SLIDE_EQ_MIN_1_USD , sum(b.SLIDE_EQ_PLS_1_USD) as SLIDE_EQ_PLS_1_USD , sum(b.SLIDE_EQ_MIN_02_USD) as SLIDE_EQ_MIN_02_USD , sum(b.SLIDE_EQ_PLS_02_USD) as SLIDE_EQ_PLS_02_USD , sum(b.SLIDE_EQ_PLS_05_USD) as SLIDE_EQ_PLS_05_USD , sum(b.SLIDE_EQ_PLS_10_USD) as SLIDE_EQ_PLS_10_USD , sum(b.SLIDE_EQ_PLS_20_USD) as SLIDE_EQ_PLS_20_USD , sum(b.SLIDE_EQ_PLS_30_USD) as SLIDE_EQ_PLS_30_USD FROM cdwuser.U_EXP_MSR b WHERE COB_DATE in ('2018-02-28','2018-01-31','2017-12-29','2017-11-30','2017-10-31','2017-09-29','2017-08-31','2017-07-31','2017-06-30','2017-05-31','2017-04-28','2017-03-31','2017-02-28') AND var_excl_fl <> 'Y' AND VERTICAL_SYSTEM <> 'PIPELINE_NY' AND PARENT_LEGAL_ENTITY = '0302(G)' AND CCC_DIVISION IN ('INSTITUTIONAL EQUITY DIVISION') GROUP BY b.COB_DATE , b.PROCESS_ID, b.POSITION_KEY , b.CCC_TAPS_COMPANY, ccc_division, CCC_PL_REPORTING_REGION ) a GROUP BY cob_date, CCC_PL_REPORTING_REGION