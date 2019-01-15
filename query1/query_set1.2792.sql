SELECT cob_date , sum(a.EQ_DELTA) as EQ_DELTA , CCC_PRODUCT_LINE, CCC_PL_REPORTING_REGION FROM ( SELECT cob_date , b.PROCESS_ID, b.POSITION_KEY , b.CCC_TAPS_COMPANY, ccc_division, CCC_PL_REPORTING_REGION ,ccc_business_area, CCC_PRODUCT_LINE, b.PRODUCT_TYPE_CODE , sum(coalesce(b.USD_DELTA, 0)) as EQ_DELTA FROM cdwuser.U_EXP_MSR b WHERE COB_DATE in ('2018-02-28','2018-01-31','2017-12-29','2017-11-30','2017-10-31','2017-09-29','2017-08-31','2017-07-31','2017-06-30','2017-05-31','2017-04-28','2017-03-31','2017-02-28') AND var_excl_fl <> 'Y' AND VERTICAL_SYSTEM <> 'PIPELINE_NY' AND PARENT_LEGAL_ENTITY = '0302(G)' AND CCC_DIVISION IN ('INSTITUTIONAL EQUITY DIVISION') GROUP BY b.COB_DATE , b.PROCESS_ID, b.POSITION_KEY , b.CCC_TAPS_COMPANY, ccc_division , ccc_business_area, b.CCC_PRODUCT_LINE, b.PRODUCT_TYPE_CODE, CCC_PL_REPORTING_REGION ) a GROUP BY cob_date, CCC_PRODUCT_LINE, CCC_PL_REPORTING_REGION