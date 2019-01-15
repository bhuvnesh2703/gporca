select      COB_DATE,     CCC_BUSINESS_AREA,     CCC_PRODUCT_LINE,     CURRENCY_OF_MEASURE,     sum(USD_IR_UNIFIED_PV01) as PV01 from     cdwuser.U_EXP_MSR a where (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-27') and A.CCC_TAPS_COMPANY in ('0362','0816') AND      a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND      a.CCC_BUSINESS_AREA IN ('CREDIT-CORPORATES') AND       CCC_BANKING_TRADING IN ('TRADING') AND     POSITION_ULT_ISSUER_PARTY_DARWIN_NAME NOT LIKE ('%MORGAN STANLEY%') group by      COB_DATE,     CCC_BUSINESS_AREA,     CCC_PRODUCT_LINE,     CURRENCY_OF_MEASURE