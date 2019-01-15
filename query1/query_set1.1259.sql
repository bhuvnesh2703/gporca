SELECT     a.COB_DATE,     CASE         WHEN a.CR_ULTIMATE_CNTRY_CODE <> 'UNDEFINED' THEN a.CR_ULTIMATE_CNTRY_CODE         WHEN a.COUNTRY_CD_OF_RISK <> 'UNDEFINED' THEN a.COUNTRY_CD_OF_RISK         WHEN a.PRODUCT_TYPE_CODE = 'CONVRT' THEN a.ISSUER_COUNTRY_CODE         WHEN a.CR_ULTIMATE_CNTRY_CODE = 'UNDEFINED' THEN a.ISSUER_COUNTRY_CODE     ELSE a.CR_ULTIMATE_CNTRY_CODE END AS ULT_COUNTRY,     SUM(a.USD_EXPOSURE) AS USD_ASSET_EXPOSURE,     SUM(a.USD_PV10_BENCH) AS USD_PV10_BENCH FROM     cdwuser.U_CR_MSR a WHERE     1 = 1 AND (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-27') and A.CCC_PL_REPORTING_REGION in ('JAPAN','ASIA PACIFIC') AND A.CCC_TAPS_COMPANY in ('0302','0347','0853','4043','4298','4863','6120','6899','6837','6893','4044','5869','0856','6325','0301','0893','0993') AND      ((a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND     a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND     a.CCC_PRODUCT_LINE = 'DISTRESSED TRADING') OR     a.CCC_DIVISION='INSTITUTIONAL SECURITIES OTHER') AND     a.VAR_EXCL_FL <> 'Y' GROUP BY     a.COB_DATE,     CASE         WHEN a.CR_ULTIMATE_CNTRY_CODE <> 'UNDEFINED' THEN a.CR_ULTIMATE_CNTRY_CODE         WHEN a.COUNTRY_CD_OF_RISK <> 'UNDEFINED' THEN a.COUNTRY_CD_OF_RISK         WHEN a.PRODUCT_TYPE_CODE = 'CONVRT' THEN a.ISSUER_COUNTRY_CODE         WHEN a.CR_ULTIMATE_CNTRY_CODE = 'UNDEFINED' THEN a.ISSUER_COUNTRY_CODE     ELSE a.CR_ULTIMATE_CNTRY_CODE END