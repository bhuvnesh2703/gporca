SELECT COB_DATE, CASE WHEN A.EM_GROUP IN ('Other', 'EM ex CNY, CNH') THEN 'EM ex CNY, CNH' ELSE A.EM_GROUP END AS EM_GROUP, A.G10_GROUP, SUM(COALESCE(USD_FX, 0)) /1000000 AS FX FROM CDWUSER.U_DM_FIRMWIDE A WHERE COB_DATE >= '07/01/2016' AND COB_DATE <= '02/28/2018' AND USD_FX <> 0 AND VAR_EXCL_FL <> 'Y' AND BU_RISK_RUN_CUSTOM1 <> 'STORAGE' AND CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA NOT IN ('COMMODITIES') AND DIVISION_GROUP = 'ISG CORE' AND CCC_BUSINESS_AREA NOT LIKE 'CPM%' AND CCC_BUSINESS_AREA NOT IN ('COMMODS FINANCING') AND NOT ( A.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP' AND SPG_DESC IN ('CMBS IO REREMIC','CMBS IO','CMBS SECURITY','RMBS PRIME RESIDUAL','CMBS INDEX','CORPORATE INDEX') AND CCC_PRODUCT_LINE IN ('CRE LENDING', 'CREL BANK HFI', 'CRE LENDING SEC/HFS') ) AND CURRENCY_OF_MEASURE NOT IN ('UBD', 'USD') GROUP BY COB_DATE, CASE WHEN A.EM_GROUP IN ('Other', 'EM ex CNY, CNH') THEN 'EM ex CNY, CNH' ELSE A.EM_GROUP END, A.G10_GROUP