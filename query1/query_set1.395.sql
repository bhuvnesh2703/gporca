select a.BU_RISK_RUN_CUSTOM1, a.CCC_PL_REPORTING_REGION, a.COB_DATE, case when a.CCC_BUSINESS_AREA = 'COMMODITIES' then a.CCC_PRODUCT_LINE else a.CCC_BUSINESS_AREA end as CCC_BUSINESS_AREA, a.BUSS_AREA_PLUS_SPREADS, a.QUARTERS, a.TIME_BUCKET_ANNUAL, a.ASSET_TYPE, a.SECTYPE, sum(a.RAW_GREEK) as RAW_GREEK FROM ( select job_cmdty_code, prod_pos_name_description, CCC_PRODUCT_LINE, product_sub_type_code, product_type_code, CMDTY_CD,EXPIRATION_DATE, time_bucket_quarter,CCC_TRD_BOOK, product_sub_type_name,COB_DATE, CCC_BUSINESS_AREA,TIME_BUCKET_CALENDAR, time_bucket_annual,CCC_STRATEGY,Location_Group, CCC_PL_REPORTING_REGION, BU_RISK_RUN_CUSTOM1, sum(cast(USD_CM_Kappa as numeric(15,5))) as dollar_greek, sum(case when PRODUCT_TYPE_CODE = 'TIMESPREAD' and PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD' then 0 else cast(RAW_CM_KAPPA as numeric(15,5)) end) as raw_greek, case when product_sub_type_name in ('STORAGE RELET', 'STORAGE CONTRACT') and COB_DATE >=EXPIRATION_DATE and BU_RISK_RUN_CUSTOM1 = 'STORAGE' then 'TANK' when product_sub_type_name in ('FLEXDEAL', 'P_TANK-T', 'PHYSICAL INVENTORY') and BU_RISK_RUN_CUSTOM1 <> 'STORAGE' then 'TANK' else 'NOTANK' end as TANK, case when EXPIRATION_DATE < ('2019-09-01') then time_bucket_quarter end as quarters, case when product_type_code = 'CRUDE' then prod_pos_name_description when product_type_code in ('DIST', 'JET') then CMDTY_CD when product_type_code = 'ETHANOL' then 'Ethanol' when product_type_code = 'GAS' then 'Gasoline' when (product_type_code= 'NGL' AND job_cmdty_code = 'PROPANESCP') then 'Propane' when (product_type_code= 'NGL' AND job_cmdty_code = 'BUTANE A KMI') then 'Butane' when (product_type_code= 'NGL' AND CMDTY_CD is not null) then CMDTY_CD end as sectype, case when BU_RISK_RUN_CUSTOM1 = 'STORAGE' then 'STORAGE' else 'NOSTORAGE' end as STORAGE, case when CMDTY_CD = 'SPREADS' then CMDTY_CD when (CCC_BUSINESS_AREA in ('OIL LIQUIDS','TMG','OLYMPUS') or CCC_PRODUCT_LINE in ('OIL & PRODUCTS')) then 'OIL LIQUIDS' else CCC_BUSINESS_AREA end as BUSS_AREA_PLUS_SPREADS, case when product_type_code in ('DIST', 'JET') then 'Distillate' when product_type_code in ('FUEL','GAS','CRUDE','NAPHTHA', 'ETHANOL', 'NGL') then product_type_code when product_type_code in ('CLEAN FREIGHT','DIRTY FREIGHT') then 'Freight' end as asset_type FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-02-27') AND ((CCC_BUSINESS_AREA NOT IN ('OIL LIQUIDS') AND CCC_DIVISION = 'COMMODITIES') OR (CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES' and CCC_PRODUCT_LINE IN ('OIL & PRODUCTS'))) AND PRODUCT_TYPE_CODE NOT IN ('CURRENCY', 'INTEREST RATE', 'INFLATION', 'TBD', 'MISC','CVA', 'FVA', 'ERROR') AND (CCC_PL_REPORTING_REGION IN ('EUROPE','EMEA')) group by job_cmdty_code, prod_pos_name_description,CCC_PRODUCT_LINE,product_sub_type_code, product_type_code,product_sub_type_name,COB_DATE, CCC_PL_REPORTING_REGION, CCC_BUSINESS_AREA,TIME_BUCKET_CALENDAR,TIME_BUCKET_ANNUAL, CMDTY_CD, EXPIRATION_DATE, Location_Group,time_bucket_quarter,CCC_TRD_BOOK, BU_RISK_RUN_CUSTOM1, CCC_STRATEGY)A GROUP BY a.BU_RISK_RUN_CUSTOM1, a.COB_DATE, a.CCC_BUSINESS_AREA,a.CCC_PL_REPORTING_REGION, a.CCC_PRODUCT_LINE, a.QUARTERS,a.BUSS_AREA_PLUS_SPREADS, a.TIME_BUCKET_ANNUAL, a.ASSET_TYPE, a.SECTYPE