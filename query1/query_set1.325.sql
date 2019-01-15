Select x.COB_DATE, x.TIME_BUCKET_CALENDAR, X.quarters, x.PRODUCT_SUB_TYPE_CODE, x.PRODUCT_TYPE_CODE, case when x.Dollarized_Delta >= 0 then 'L' else 'S' end as Long_Short_Flag, sum(x.Dollarized_Delta) as Dolla_Delta From ( SELECT a.COB_DATE, a.POSITION_ID, a.TIME_BUCKET_CALENDAR, case when A.PRODUCT_SUB_TYPE_CODE in ('N EAST CONSUMPTI', 'N EAST CONSUMPTION') then 'N EAST CONSUMPTI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('N EAST APPALACHI', 'N EAST APPALACHIA') THEN 'N EAST APPALACHI' ELSE A.PRODUCT_SUB_TYPE_CODE END AS PRODUCT_SUB_TYPE_CODE, case when a.EXPIRATION_DATE < ('2019-09-01') then time_bucket_quarter end as quarters, a.PRODUCT_TYPE_CODE, sum(a.USD_CM_DELTA) as Dollarized_Delta FROM cdwuser.U_CM_MSR a WHERE ((a.CCC_DIVISION='COMMODITIES' AND a.CCC_BUSINESS_AREA IN ('NA ELECTRICITYNATURAL GAS')) /*OLG LOGIC*/ OR (A.CCC_DIVISION = 'FIXED INCOME DIVISION' AND A.CCC_BUSINESS_AREA = 'COMMODITIES' and a.CCC_PRODUCT_LINE IN ('NA POWER & GAS'))) /* NEW LOGIC*/ AND a.PRODUCT_TYPE_CODE in ('NATGAS') AND a.COB_DATE IN ('2018-02-28','2018-01-31','2017-12-29','2017-09-29','2017-06-30','2017-03-31') and NOT(a.INCLUDE_IN_REG_CAAP_FL = 'N' and a.PRODUCT_SUB_TYPE_CODE in ('N EAST CONSUMPTI', 'N EAST CONSUMPTION', 'N EAST APPALACHI', 'N EAST APPALACHIA') and a.BOOK = '18003') group by a.COB_DATE, a.PRODUCT_TYPE_CODE, a.PROD_POS_NAME_DESCRIPTION, a.TIME_BUCKET_CALENDAR, a.FUTURES_EQUIVALENT_DATE, a.PROD_POS_NAME_DESCRIPTION, a.TIME_BUCKET_QUARTER, a.PRODUCT_SUB_TYPE_CODE, a.POSITION_ID, case when a.EXPIRATION_DATE < ('2019-09-01') then time_bucket_quarter end )X group by x.COB_DATE, x.TIME_BUCKET_CALENDAR, X.quarters, x.PRODUCT_SUB_TYPE_CODE, x.PRODUCT_TYPE_CODE, case when x.Dollarized_Delta >= 0 then 'L' else 'S' end