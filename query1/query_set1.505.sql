SELECT a.Cob_date, a.TIME_BUCKET_CALENDAR, sum(cast(a.USD_CM_Delta as numeric(15,5))) as Dollar_Delta, sum(case when a.PRODUCT_TYPE_CODE = 'TIMESPREAD' and A.PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD' then 0 else cast(a.RAW_CM_DELTA as numeric(15,5)) end) as Raw_Delta, sum(case when a.PRODUCT_TYPE_CODE = 'TIMESPREAD' and A.PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD' then 0 else cast(a.RAW_CM_KAPPA as numeric(15,5)) end) as Raw_Vega, CASE WHEN a.Product_Type_Code IN ('DIST', 'GAS') THEN 'DISTILLATE' WHEN a.Product_Type_Code IN ('JET', 'CRUDE') THEN Product_Type_Code WHEN a.Product_Type_Code IN ('CLEAN FREIGHT', 'DIRTY FREIGHT') THEN 'Freight' ELSE 'OTHER' END AS product_type FROM cdwuser.U_CM_MSR a WHERE ((CCC_BUSINESS_AREA NOT IN ('OIL LIQUIDS') AND CCC_DIVISION = 'COMMODITIES') OR (CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES' and CCC_PRODUCT_LINE IN ('OIL & PRODUCTS'))) AND PRODUCT_TYPE_CODE NOT IN ('CURRENCY', 'INTEREST RATE', 'INFLATION', 'TBD', 'MISC','CVA', 'FVA', 'ERROR') AND (CCC_PL_REPORTING_REGION IN ('EUROPE','EMEA')) AND A.COB_DATE IN ('2018-02-28','2018-01-31') GROUP BY a.Cob_date, a.TIME_BUCKET_CALENDAR, CASE WHEN a.Product_Type_Code IN ('DIST', 'GAS') THEN 'DISTILLATE' WHEN a.Product_Type_Code IN ('JET', 'CRUDE') THEN Product_Type_Code WHEN a.Product_Type_Code IN ('CLEAN FREIGHT', 'DIRTY FREIGHT') THEN 'Freight' ELSE 'OTHER' END