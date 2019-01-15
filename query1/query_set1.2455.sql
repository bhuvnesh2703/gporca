SELECT SUM(USD_IR_UNIFIED_PV01),PRODUCT_TYPE_CODE,CCC_STRATEGY, TERM_BUCKET, COB_DATE,CCC_BUSINESS_AREA,CURVE_NAME,CURRENCY_OF_MEASURE FROM CDWUSER.U_EXP_MSR V where COB_DATE='2018-02-28' AND v.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION' AND NOT PRODUCT_DESCRIPTION LIKE '%BLFT%' AND PRODUCT_DESCRIPTION <>'UNDEFINED' AND (CURRENCY_OF_MEASURE like '%USD%' or CURRENCY_OF_MEASURE like '%UBD%'   or CURRENCY_OF_MEASURE like '%BR%') AND CCC_BUSINESS_AREA NOT LIKE '%CPM%' AND  (USD_IR_UNIFIED_PV01<>0 )  GROUP BY COB_DATE,PRODUCT_TYPE_CODE,TERM_BUCKET,CCC_BUSINESS_AREA, CURVE_NAME,CCC_STRATEGY,CURRENCY_OF_MEASURE