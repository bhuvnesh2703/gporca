SELECT CASE WHEN A.CCC_BUSINESS_AREA = 'COMMODITIES' THEN A.CCC_PRODUCT_LINE ELSE A.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, A.COB_DATE, A.TIME_BUCKET_QUARTER, A.TIME_BUCKET_CALENDAR, sum(USD_CM_Delta) as dollar_delta, sum(case when (a.PRODUCT_TYPE_CODE = 'TIMESPREAD' and a.PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD' ) OR a.product_type_code in ('CLEAN FREIGHT','DIRTY FREIGHT','COAL','DRY FREIGHT','EMISSIONS-EU','IRON ORE','NATGAS','PLASTICS') then 0 else cast(a.RAW_CM_DELTA as numeric(15,5)) end) as raw_delta, A.PRODUCT_TYPE_CODE, case when A.PRODUCT_TYPE_CODE in ('DIST', 'JET') then 'Distillate' when A.PRODUCT_TYPE_CODE in ('FUEL','GAS','CRUDE','NAPHTHA', 'ETHANOL', 'NGL') then product_type_code when (a.PRODUCT_TYPE_CODE = 'TIMESPREAD' and a.PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD' ) then 'CRUDE' else 'other' end as product_type, case when A.PRODUCT_SUB_TYPE_NAME in('STORAGE RELET', 'STORAGE CONTRACT') and A.COB_DATE>= a.FUTURES_EQUIVALENT_DATE and A.BU_RISK_RUN_CUSTOM1 ='STORAGE' then 'Tank' when A.PRODUCT_SUB_TYPE_NAME in('FLEXDEAL','P_TANK-T' ,'PHYSICAL INVENTORY') and A.BU_RISK_RUN_CUSTOM1 <>'STORAGE' then 'Tank' else 'NoTank' end as Tank, A.BU_RISK_RUN_CUSTOM1 FROM cdwuser.U_CM_MSR A WHERE ((A.CCC_DIVISION='COMMODITIES' AND A.CCC_BUSINESS_AREA IN ('OIL LIQUIDS', 'TMG', 'OLYMPUS')) /*OLG LOGIC*/ OR (A.CCC_DIVISION = 'FIXED INCOME DIVISION' AND A.CCC_BUSINESS_AREA = 'COMMODITIES' and (a.CCC_PRODUCT_LINE IN ('OIL & PRODUCTS')OR A.CCC_STRATEGY IN ('TMG', 'OLYMPUS')))) /*NEW LOGIC*/ AND A.PRODUCT_TYPE_CODE NOT IN ('CURRENCY', 'INTEREST RATE', 'INFLATION', 'TBD', 'MISC','CVA', 'FVA', 'ERROR') AND A.COB_DATE IN ('2018-02-28','2018-01-31','2017-12-29','2017-09-29','2017-06-30','2017-03-31') Group By A.CCC_BUSINESS_AREA, A.BU_RISK_RUN_CUSTOM1, A.COB_DATE, A.FUTURES_EQUIVALENT_DATE, A.PRODUCT_TYPE_CODE, A.PRODUCT_SUB_TYPE_NAME, A.TIME_BUCKET_QUARTER, A.TIME_BUCKET_CALENDAR, A.CCC_PRODUCT_LINE, a.PROD_POS_NAME_DESCRIPTION