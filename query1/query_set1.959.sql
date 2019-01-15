SELECT     a.COB_DATE,     a.CCC_BUSINESS_AREA,     a.CCC_STRATEGY,     CASE WHEN term_new <= 1.5 THEN '0-1YR' WHEN     term_new > 1.5 AND     term_new <= 6.5     THEN '2-6YR' WHEN     term_new > 6.5 AND     term_new <= 13.5     THEN '7-14YR' WHEN     term_new > 13.5 AND     term_new <= 30.5     THEN '15-30YR' WHEN term_new > 30.5 THEN '30Yr+'     ELSE 'NULL' END AS term_new_group, Case WHEN FEED_SOURCE_NAME = 'CORISK' THEN substr(PROD_POS_NAME_DESCRIPTION,1,3)   ELSE a.CURRENCY_OF_MEASURE END AS CURRENCY,     CASE WHEN PRODUCT_SUB_TYPE_CODE IN ('MPE_FVA_RAW', 'MPE_FVA') THEN 'MPE_FVA' WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_FVA', 'MNE_FVA_NET'         ) THEN 'MNE_FVA_NET' WHEN (PRODUCT_SUB_TYPE_CODE IN ('MPE_CVA', 'MPE', 'MPE_PROXY', 'MNE_CP') OR                                    (BOOK IN ('CV2LP', 'CV2LN', 'CVPL1', 'CV2LD', 'CVPL2', 'FVPL2') AND                                     BU_RISK_SYSTEM LIKE 'STS%') OR                                    BOOK = '1679') THEN 'MPE' WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_CVA', 'MNE') THEN 'MNE'     ELSE 'Hedge' END AS TYPE_FLAG,     SUM (a.USD_IR_UNIFIED_PV01) AS USD_IR_UNIFIED_PV01 FROM cdwuser.U_IR_MSR_INTRPLT a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-27') and      (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT','MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR      a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) AND a.ccc_product_line NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE') AND a.USD_IR_UNIFIED_PV01 is not null GROUP BY     a.COB_DATE,     a.CCC_BUSINESS_AREA,     a.CCC_STRATEGY,     term_new_group,     CURRENCY,     TYPE_FLAG