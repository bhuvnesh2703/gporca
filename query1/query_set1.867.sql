SELECT a.COB_DATE, a.CCC_BUSINESS_AREA, PRODUCT_SUB_TYPE_CODE AS MPE_FLAG, SUM(a.USD_MARKET_VALUE) as USD_MARKET_VALUE FROM cdwuser.U_DM_CVA a WHERE  (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-01') and IS_UK_GROUP in ('Y') AND  (a.CCC_BUSINESS_AREA IN ('CPM', 'CPM TRADING (MPE)','CREDIT') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) AND a.PRODUCT_SUB_TYPE_CODE IN ('LVA','MPE_FVA_PROXY','MPE_FVA', 'MNE_FVA_NET') and a.ccc_product_line not in ('CREDIT LOAN PORTFOLIO','CMD STRUCTURED FINANCE') GROUP BY a.COB_DATE, a.CCC_BUSINESS_AREA, PRODUCT_SUB_TYPE_CODE