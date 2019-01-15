Select case when CCC_BUSINESS_AREA = 'COMMODITIES' then CCC_PRODUCT_LINE else CCC_BUSINESS_AREA end as CCC_BUSINESS_AREA, A.CCC_PL_REPORTING_REGION, A.COB_DATE, A.PRODUCT_TYPE_CODE, CASE WHEN A.PRODUCT_SUB_TYPE_CODE IN ('N EAST CONSUMPTI', 'N EAST CONSUMPTION') THEN 'N EAST CONSUMPTI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('N EAST APPALACHI', 'N EAST APPALACHIA') THEN 'N EAST APPALACHI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('POLYPROPELENE HO', 'POLYPROPELENE HOMOPOLYMER') THEN 'POLYPROPELENE HO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('POLYPROPYLENE CO', 'POLYPROPYLENE COPOLYMER') THEN 'POLYPROPYLENE CO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('PURE TEREPHTHALI', 'PURE TEREPHTHALIC ACID') THEN 'PURE TEREPHTHALI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GSCI-PRECIOUS ME', 'GSCI-PRECIOUS METALS') THEN 'GSCI-PRECIOUS ME' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('ZERO PRICED EXPO', 'ZERO PRICED EXPOSURE') THEN 'ZERO PRICED EXPO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('DJUBS-BASE METAL', 'DJUBS-BASE METALS') THEN 'DJUBS-BASE METAL' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GAS SULFUR CREDI', 'GAS SULFUR CREDIT') THEN 'GAS SULFUR CREDI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GASOIL TIMESPREA', 'GASOIL TIMESPREAD') THEN 'GASOIL TIMESPREA' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('USD INTEREST RAT', 'USD INTEREST RATE') THEN 'USD INTEREST RAT' ELSE A.PRODUCT_SUB_TYPE_CODE END AS PRODUCT_SUB_TYPE_CODE, A.TIME_BUCKET_CALENDAR, SUM(A.DOLLAR_GREEK) as DOLLAR_GREEK, SUM(A.RAW_GREEK) as RAw_GREEK From ( Select prod_pos_name_description, COB_DATE, ccc_business_area,CCC_PL_REPORTING_REGION, ccc_product_line, product_type_code,time_bucket_calendar, product_sub_type_code, TIME_BUCKET_ANNUAL,EXPIRATION_DATE,time_bucket_quarter, sum(cast(USD_CM_Delta as numeric(15,5))) as dollar_greek, sum(cast(RAW_CM_Delta as numeric(15,5))) as raw_greek, case when product_sub_type_code ='ALUMINUM-20MT' then 'Aluminum' when product_type_code in ('BASEMETAL', 'PRECIOUSMETAL', 'FUND') then product_sub_type_code WHEN CCC_PRODUCT_LINE IN ('PRECIOUS METALS') OR CCC_STRATEGY IN ('BULKS', 'BASE METALS') THEN product_sub_type_code end as Metal FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-02-27') AND ((CCC_BUSINESS_AREA IN ('METALS') AND CCC_DIVISION IN ('COMMODITIES')) /*OLD LOGIC*/ OR (CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA IN ('COMMODITIES') and CCC_PRODUCT_LINE IN ('PRECIOUS METALS') OR CCC_STRATEGY IN ('BULKS', 'BASE METALS') )) /*NEW LOGIC*/ AND (CCC_PL_REPORTING_REGION IN ('EUROPE','EMEA')) Group By prod_pos_name_description, COB_DATE,ccc_business_area,CCC_PL_REPORTING_REGION, ccc_product_line, ccc_strategy, product_type_code,time_bucket_calendar,TIME_BUCKET_ANNUAL, EXPIRATION_DATE,time_bucket_quarter, product_sub_type_code)A GROUP BY A.CCC_BUSINESS_AREA, A.COB_DATE, A.CCC_PL_REPORTING_REGION, CCC_PRODUCT_LINE, A.PRODUCT_TYPE_CODE, A.PRODUCT_SUB_TYPE_CODE, A.TIME_BUCKET_CALENDAR