SELECT a.COB_DATE ,a.CCC_PL_REPORTING_REGION ,a.STORAGE ,a.ASSET_TYPE ,a.TIME_BUCKET_CALENDAR ,CASE WHEN ( a.CCC_BUSINESS_AREA_GROUP IN ('NA ELECTRICITYNATURAL GAS') OR A.CCC_PRODUCT_LINE IN ('NA POWER & GAS') ) THEN 'NA ELECTRICITYNATURAL GAS' WHEN ( ( A.CCC_BUSINESS_AREA IN ('OIL LIQUIDS') AND A.CCC_PRODUCT_LINE NOT IN ('LEGACY OIL') ) OR A.CCC_PRODUCT_LINE IN ('OIL & PRODUCTS') ) THEN 'OIL LIQUIDS' WHEN CCC_PRODUCT_LINE IN ('EU POWER & GAS') THEN 'EU POWER & GAS' ELSE 'COMMODITIES OTHER' END AS CCC_BUSINESS_AREA_GROUP ,a.SECTYPE ,a.TIME_BUCKET_ANNUAL ,a.QUARTERS ,a.TANK ,SUM(a.RAW_GREEK) AS RAW_GREEK FROM ( SELECT job_cmdty_code ,prod_pos_name_description ,CCC_PRODUCT_LINE ,product_sub_type_code ,product_type_code ,CMDTY_CD ,EXPIRATION_DATE ,time_bucket_quarter ,CCC_TRD_BOOK ,product_sub_type_name ,COB_DATE ,CCC_BUSINESS_AREA ,TIME_BUCKET_CALENDAR ,TIME_BUCKET_ANNUAL ,CCC_STRATEGY ,CCC_PL_REPORTING_REGION ,BU_RISK_RUN_CUSTOM1 ,SUM(CAST(RAW_CM_DELTA AS NUMERIC(15, 5)))AS raw_greek ,CASE WHEN CCC_BUSINESS_AREA IN ('OIL LIQUIDS', 'TMG', 'OLYMPUS') THEN 'OIL LIQUIDS' ELSE CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA_GROUP ,CASE WHEN product_sub_type_name IN ('STORAGE RELET', 'STORAGE CONTRACT') AND COB_DATE >= EXPIRATION_DATE AND BU_RISK_RUN_CUSTOM1 = 'STORAGE' THEN 'TANK' WHEN product_sub_type_name IN ('FLEXDEAL', 'P_TANK-T', 'PHYSICAL INVENTORY') AND BU_RISK_RUN_CUSTOM1 <> 'STORAGE' THEN 'TANK' ELSE 'NOTANK' END AS TANK ,CASE when EXPIRATION_DATE < ('2019-09-01') THEN time_bucket_quarter END AS quarters ,CASE WHEN product_type_code = 'CRUDE' THEN prod_pos_name_description WHEN product_type_code = 'DIST' THEN CMDTY_CD WHEN product_type_code = 'JET' THEN 'JET FUELS' WHEN product_type_code = 'ETHANOL' THEN 'Ethanol' WHEN product_type_code = 'GAS' THEN 'Gasoline' WHEN ( product_type_code = 'NGL' AND job_cmdty_code = 'PROPANESCP' ) THEN 'Propane' WHEN ( product_type_code = 'NGL' AND job_cmdty_code = 'BUTANE A KMI' ) THEN 'Butane' WHEN ( product_type_code = 'NGL' AND CMDTY_CD IS NOT NULL ) THEN CMDTY_CD END AS sectype ,CASE WHEN BU_RISK_RUN_CUSTOM1 = 'STORAGE' THEN 'STORAGE' ELSE 'NOSTORAGE' END AS STORAGE ,CASE WHEN product_type_code IN ('DIST') THEN 'Distillate' WHEN product_type_code IN ('FUEL', 'GAS', 'CRUDE', 'NAPHTHA', 'ETHANOL', 'NGL', 'JET') THEN product_type_code WHEN product_type_code IN ('CLEAN FREIGHT', 'DIRTY FREIGHT') THEN 'Freight' END AS asset_type FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-01-31') and PRODUCT_TYPE_CODE = 'TIMESPREAD' AND PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD' AND ( ( CCC_BUSINESS_AREA NOT IN ('OIL LIQUIDS') AND CCC_DIVISION = 'COMMODITIES' ) OR ( CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES' AND CCC_PRODUCT_LINE IN ('OIL & PRODUCTS') ) ) AND PRODUCT_TYPE_CODE NOT IN ('CURRENCY', 'INTEREST RATE', 'INFLATION', 'TBD', 'MISC', 'CVA', 'FVA', 'ERROR') GROUP BY job_cmdty_code ,prod_pos_name_description ,CCC_PRODUCT_LINE ,product_sub_type_code ,product_type_code ,product_sub_type_name ,COB_DATE ,CCC_PL_REPORTING_REGION ,CCC_BUSINESS_AREA ,TIME_BUCKET_CALENDAR ,TIME_BUCKET_ANNUAL ,CMDTY_CD ,EXPIRATION_DATE ,time_bucket_quarter ,CCC_TRD_BOOK ,BU_RISK_RUN_CUSTOM1 ,CCC_STRATEGY ) a GROUP BY a.COB_DATE ,a.CCC_PL_REPORTING_REGION ,a.STORAGE ,a.ASSET_TYPE ,CASE WHEN ( a.CCC_BUSINESS_AREA_GROUP IN ('NA ELECTRICITYNATURAL GAS') OR A.CCC_PRODUCT_LINE IN ('NA POWER & GAS') ) THEN 'NA ELECTRICITYNATURAL GAS' WHEN ( ( A.CCC_BUSINESS_AREA IN ('OIL LIQUIDS') AND A.CCC_PRODUCT_LINE NOT IN ('LEGACY OIL') ) OR A.CCC_PRODUCT_LINE IN ('OIL & PRODUCTS') ) THEN 'OIL LIQUIDS' WHEN CCC_PRODUCT_LINE IN ('EU POWER & GAS') THEN 'EU POWER & GAS' ELSE 'COMMODITIES OTHER' END ,a.SECTYPE ,a.TIME_BUCKET_ANNUAL ,a.QUARTERS ,a.TANK ,a.TIME_BUCKET_CALENDAR