SELECT     a.COB_DATE,     a.CCC_BUSINESS_AREA,  CASE WHEN A.PRODUCT_SUB_TYPE_CODE IN ('CTDVA_FLRVA', 'LVA_FLRVA') THEN 'FLRVA' ELSE A.MPE_FLAG END AS MPE_FLAG,     SUM (a.USD_MARKET_VALUE) AS USD_MARKET_VALUE FROM cdwuser.U_DM_CVA a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and CCC_PL_REPORTING_REGION in ('EMEA') AND      (A.CCC_BUSINESS_AREA IN ('CPM', 'CPM TRADING (MPE)', 'CREDIT') OR      A.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) AND     A.PRODUCT_SUB_TYPE_CODE IN ('LVA', 'MPE_FVA_PROXY', 'MPE', 'MPE_CVA', 'MPE_PROXY', 'MNE', 'MNE CVA',                                                      'MNE_CP', 'MPE_FVA', 'MPE_FVA_RAW', 'MNE_FVA_NET','MNE_FVA', 'CTDVA', 'OISVA', 'CTDVA_FLRVA', 'LVA_FLRVA') AND      A.Ccc_product_line not in ('CREDIT LOAN PORTFOLIO','CMD STRUCTURED FINANCE') GROUP BY     A.COB_DATE,     A.CCC_BUSINESS_AREA,     A.MPE_FLAG,  CASE WHEN A.PRODUCT_SUB_TYPE_CODE IN ('CTDVA_FLRVA', 'LVA_FLRVA') THEN 'FLRVA' ELSE A.MPE_FLAG END