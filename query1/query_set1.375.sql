Select A.COB_Date, A.CCC_PL_REPORTING_REGION, a.PRODUCT_SUB_TYPE_CODE, A.PRODUCT_TYPE_CODE, A.TIME_BUCKET_CALENDAR, A.QUARTERS, A.ANCILLARIES, Sum(A.DOLLAR_GREEK) as DOLLAR_GREEK, SUM (A.raw_greek) as RAW_GREEK FROM( Select prod_pos_name_description,CCC_PL_REPORTING_REGION,COB_DATE,product_type_code, time_bucket_calendar, product_sub_type_code, TIME_BUCKET_ANNUAL,EXPIRATION_DATE,time_bucket_quarter, sum(cast(USD_CM_Delta as numeric(15,5))) as dollar_greek, sum(cast(RAW_CM_Delta as numeric(15,5))) as raw_greek, case when PRODUCT_TYPE_CODE = 'ANCILLARIES' then 'ERCOT-ANCIL' when PRODUCT_TYPE_CODE = 'ICAP' then 'ICAP' when PRODUCT_TYPE_CODE = 'EMISSIONS-US' and PROD_POS_NAME_DESCRIPTION = 'CCA-CA' then 'CCA-CA' when PRODUCT_TYPE_CODE = 'EMISSIONS-US' and PROD_POS_NAME_DESCRIPTION = 'RGGI' then 'RGGI' when PRODUCT_TYPE_CODE = 'EMISSIONS-US' and PROD_POS_NAME_DESCRIPTION not in ('RGGI','CCA-CA') then 'Other US-EMISSIONS' else 'OTHER' end as ANCILLARIES, case when EXPIRATION_DATE < ('2019-09-01') then time_bucket_quarter end as quarters FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-02-27') AND ((CCC_BUSINESS_AREA in ('NA ELECTRICITYNATURAL GAS') AND CCC_DIVISION = 'COMMODITIES' ) /*OLD LOGIC*/ OR (CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES' and CCC_PRODUCT_LINE IN ('NA POWER & GAS'))) /*NEW LOGIC*/ AND product_type_code not in ('NATGAS','EAST OFF', 'EAST PEAK', 'MIDWEST OFF', 'MIDWEST PEAK', 'TEXAS OFF', 'TEXAS PEAK', 'WEST OFF', 'WEST PEAK','GRNPWR OFF' ,/*'GRNPWR PEAK',*/ 'EAST INTERCONNECT OF','EAST INTERCONNECT PE','TEXAS INTERCONNECT O', 'TEXAS INTERCONNECT P', 'ERCOT', 'WEST INTERCONNECT OF', 'WEST INTERCONNECT PE') AND (CCC_PL_REPORTING_REGION IN ('EUROPE','EMEA')) AND NOT(INCLUDE_IN_REG_CAAP_FL = 'N' and PRODUCT_SUB_TYPE_CODE in ('N EAST CONSUMPTI', 'N EAST CONSUMPTION', 'N EAST APPALACHI', 'N EAST APPALACHIA') and BOOK = '18003') Group By prod_pos_name_description,COB_DATE,CCC_PL_REPORTING_REGION,PRODUCT_TYPE_CODE,time_bucket_calendar,TIME_BUCKET_ANNUAL, EXPIRATION_DATE,time_bucket_quarter, product_sub_type_code,PROD_POS_NAME_DESCRIPTION)A Group By A.COB_Date, A.PRODUCT_TYPE_CODE, A.CCC_PL_REPORTING_REGION, A.TIME_BUCKET_CALENDAR, A.QUARTERS, A.ANCILLARIES ,a.PRODUCT_SUB_TYPE_CODE