Select A.COB_DATE, A.CCC_PL_REPORTING_REGION, CASE WHEN A.PRODUCT_SUB_TYPE_CODE IN ('N EAST CONSUMPTI', 'N EAST CONSUMPTION') THEN 'N EAST CONSUMPTI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('N EAST APPALACHI', 'N EAST APPALACHIA') THEN 'N EAST APPALACHI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('POLYPROPELENE HO', 'POLYPROPELENE HOMOPOLYMER') THEN 'POLYPROPELENE HO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('POLYPROPYLENE CO', 'POLYPROPYLENE COPOLYMER') THEN 'POLYPROPYLENE CO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('PURE TEREPHTHALI', 'PURE TEREPHTHALIC ACID') THEN 'PURE TEREPHTHALI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GSCI-PRECIOUS ME', 'GSCI-PRECIOUS METALS') THEN 'GSCI-PRECIOUS ME' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('ZERO PRICED EXPO', 'ZERO PRICED EXPOSURE') THEN 'ZERO PRICED EXPO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('DJUBS-BASE METAL', 'DJUBS-BASE METALS') THEN 'DJUBS-BASE METAL' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GAS SULFUR CREDI', 'GAS SULFUR CREDIT') THEN 'GAS SULFUR CREDI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GASOIL TIMESPREA', 'GASOIL TIMESPREAD') THEN 'GASOIL TIMESPREA' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('USD INTEREST RAT', 'USD INTEREST RATE') THEN 'USD INTEREST RAT' ELSE PRODUCT_SUB_TYPE_CODE END AS PRODUCT_SUB_TYPE_CODE, A.STORAGE, A.LOCATION_GROUP, A.ASSET_TYPE, A.SECTYPE, A.TIME_BUCKET_ANNUAL, case when a.CCC_BUSINESS_AREA in ('NA ELECTRICITYNATURAL GAS') OR A.CCC_PRODUCT_LINE IN ('NA POWER & GAS') THEN 'NA ELECTRICITYNATURAL GAS' WHEN ((A.CCC_BUSINESS_AREA in ('OIL LIQUIDS') AND A.CCC_PRODUCT_LINE NOT IN ('LEGACY OIL')) OR A.CCC_PRODUCT_LINE IN ('OIL & PRODUCTS'))THEN 'OIL LIQUIDS' ELSE 'COMMODITIES OTHER' END AS CCC_BUSINESS_AREA_GROUP, A.QUARTERS, A.TANK, SUM(A.RAW_GREEK) as RAW_GREEK FROM ( select job_cmdty_code, prod_pos_name_description, LOCATION_GROUP, CCC_PRODUCT_LINE, product_sub_type_code, product_type_code, CMDTY_CD,EXPIRATION_DATE,time_bucket_quarter,CCC_TRD_BOOK, product_sub_type_name,COB_DATE,CCC_BUSINESS_AREA,TIME_BUCKET_CALENDAR, TIME_BUCKET_ANNUAL,CCC_STRATEGY,CCC_PL_REPORTING_REGION, BU_RISK_RUN_CUSTOM1, sum(cast(USD_CM_Delta as numeric(15,5))) as dollar_greek, sum(case when PRODUCT_TYPE_CODE = 'TIMESPREAD' and PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD' then 0 else cast(RAW_CM_DELTA as numeric(15,5)) end) as raw_greek, case when product_sub_type_name in ('STORAGE RELET', 'STORAGE CONTRACT') and COB_DATE >=EXPIRATION_DATE and BU_RISK_RUN_CUSTOM1 = 'STORAGE' then 'TANK' when product_sub_type_name in ('FLEXDEAL', 'P_TANK-T', 'PHYSICAL INVENTORY') and BU_RISK_RUN_CUSTOM1 <> 'STORAGE' then 'TANK' else 'NOTANK' end as TANK, case when EXPIRATION_DATE < ('2019-09-01') then time_bucket_quarter end as quarters, case when product_type_code = 'CRUDE' then prod_pos_name_description when product_type_code in ('DIST', 'JET') then CMDTY_CD when product_type_code = 'ETHANOL' then 'Ethanol' when product_type_code = 'GAS' then 'Gasoline' when (product_type_code= 'NGL' AND job_cmdty_code = 'PROPANESCP') then 'Propane' when (product_type_code= 'NGL' AND job_cmdty_code = 'BUTANE A KMI') then 'Butane' when (product_type_code= 'NGL' AND CMDTY_CD is not null) then CMDTY_CD end as sectype, case when BU_RISK_RUN_CUSTOM1 = 'STORAGE' then 'STORAGE' else 'NOSTORAGE' end as STORAGE, case when product_type_code in ('DIST', 'JET') then 'Distillate' when product_type_code in ('FUEL','GAS','CRUDE','NAPHTHA', 'ETHANOL', 'NGL') then product_type_code when product_type_code in ('CLEAN FREIGHT','DIRTY FREIGHT') then 'Freight' end as asset_type FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-01-31') AND ( (CCC_BUSINESS_AREA NOT IN ('CREDIT', 'MS CVA MNE - COMMOD') AND CCC_DIVISION = 'COMMODITIES' and CCC_BUSINESS_AREA in ('OIL LIQUIDS')) /*OLD LOGIC*/ OR (CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES' and CCC_STRATEGY NOT IN ('MS CVA MNE - COMMOD') and CCC_PRODUCT_LINE IN ('OIL & PRODUCTS')) /*NEW LOGIC*/ ) AND PRODUCT_TYPE_CODE NOT IN ('CURRENCY', 'INTEREST RATE', 'INFLATION', 'TBD', 'MISC','CVA', 'FVA', 'ERROR') group by job_cmdty_code, LOCATION_GROUP, prod_pos_name_description,CCC_PRODUCT_LINE, product_sub_type_code,product_type_code,product_sub_type_name,COB_DATE, CCC_PL_REPORTING_REGION, CCC_BUSINESS_AREA,TIME_BUCKET_CALENDAR,TIME_BUCKET_ANNUAL, CMDTY_CD, EXPIRATION_DATE,time_bucket_quarter,CCC_TRD_BOOK, BU_RISK_RUN_CUSTOM1, CCC_STRATEGY)A group by A.COB_DATE, A.PRODUCT_SUB_TYPE_CODE, A.LOCATION_GROUP, A.STORAGE, A.CCC_PL_REPORTING_REGION, A.ASSET_TYPE, A.SECTYPE, A.TIME_BUCKET_ANNUAL, A.TANK, A.CCC_BUSINESS_AREA, A.QUARTERS, A.CCC_PRODUCT_LINE