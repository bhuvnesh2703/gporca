SELECT     a.COB_DATE,     a.CURRENCY_OF_MEASURE,     SUM (a.USD_IR_UNIFIED_PV01) AS USD_IR_UNIFIED_PV01 FROM cdwuser.U_IR_MSR a WHERE     1 = 1 AND (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-21') and     ((1 = 1 AND       ( A.CCC_PL_REPORTING_REGION in ('ASIA PACIFIC') AND  1=1       )) OR      a.BOOK IN ('FXJVILDDIC')) AND     (a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND      a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND      a.CCC_PRODUCT_LINE = 'DISTRESSED TRADING') AND     (a.CCC_STRATEGY NOT LIKE '%NON MSMS%') AND     a.BOOK NOT IN ('TK DISTRESSED TRADING SCOPE OUT-TKSG3', 'LAF - JP - HFS MSSFI NIG LOANS-LHSDM') AND     (a.VAR_EXCL_FL <> 'Y' AND      a.MEASURE_VAR_EXCL_FL <> 'Y') AND     a.USD_IR_UNIFIED_PV01 IS NOT NULL GROUP BY     a.COB_DATE,     a.CURRENCY_OF_MEASURE HAVING SUM (a.USD_IR_UNIFIED_PV01) <> 0