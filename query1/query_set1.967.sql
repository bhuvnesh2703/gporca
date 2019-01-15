SELECT     B.CCC_PRODUCT_LINE,     b.CURRENCY_OF_MEASURE,     SUM(B.FX_KAPPA) AS FX_KAPPA,     ABS(SUM(B.FX_KAPPA)) AS ABS_FX_KAPPA FROM     (     SELECT         A.COB_DATE,         CASE         When substr(CURVE_CURRENCY_PAIR,1,3) <> 'USD' then             substr(CURVE_CURRENCY_PAIR,1,3)         else              substr(CURVE_CURRENCY_PAIR,4,3)         end as CURRENCY_OF_MEASURE,                 A.CURVE_CURRENCY_PAIR,         A.CCC_PRODUCT_LINE,         CASE              WHEN (a.BOOK = 'COLLA' and a.VERTICAL_SYSTEM like 'PERSIST%') THEN 'CTDVA'               WHEN (a.BOOK = 'COLVA' and a.VERTICAL_SYSTEM like 'PERSIST%') THEN 'LVA'             WHEN PRODUCT_SUB_TYPE_CODE IN ('MPE_FVA_RAW', 'MPE_FVA') THEN 'MPE FVA'             WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_FVA', 'MNE_FVA_NET') THEN 'MNE FVA'             WHEN (PRODUCT_SUB_TYPE_CODE IN ('MPE_CVA','MPE', 'MPE_PROXY', 'MNE_CP') OR                  (BOOK IN ('CV2LP', 'CV2LN', 'CVPL1', 'CV2LD', 'CVPL2', 'FVPL2') AND BU_RISK_SYSTEM LIKE 'STS%') OR                  BOOK = '1679') THEN 'MPE CVA'             WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_CVA', 'MNE') THEN 'MNE CVA'             ELSE 'Hedge'         END AS TYPE_FLAG,         SUM (a.USD_FX_KAPPA) AS FX_KAPPA     FROM          cdwuser.U_FX_MSR a     WHERE (a.cob_date = '2018-02-28') AND CCC_TAPS_COMPANY in ('0302') AND          (A.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT','MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR         A.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) AND         A.CCC_PRODUCT_LINE NOT IN ('CREDIT LOAN PORTFOLIO','CMD STRUCTURED FINANCE') AND          A.USD_FX_KAPPA IS NOT NULL AND         A.CCC_PRODUCT_LINE IN ('XVA HEDGING')     GROUP BY         A.COB_DATE,         A.CURVE_CURRENCY_PAIR,         A.CCC_PRODUCT_LINE,         CASE              WHEN (a.BOOK = 'COLLA' and a.VERTICAL_SYSTEM like 'PERSIST%') THEN 'CTDVA'             WHEN (a.BOOK = 'COLVA' and a.VERTICAL_SYSTEM like 'PERSIST%') THEN 'LVA'             WHEN PRODUCT_SUB_TYPE_CODE IN ('MPE_FVA_RAW', 'MPE_FVA') THEN 'MPE FVA'             WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_FVA', 'MNE_FVA_NET') THEN 'MNE FVA'             WHEN (PRODUCT_SUB_TYPE_CODE IN ('MPE_CVA','MPE', 'MPE_PROXY', 'MNE_CP') OR              (BOOK IN ('CV2LP', 'CV2LN', 'CVPL1', 'CV2LD', 'CVPL2', 'FVPL2') AND BU_RISK_SYSTEM LIKE 'STS%') OR              BOOK = '1679') THEN 'MPE CVA'             WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_CVA', 'MNE') THEN 'MNE CVA'             ELSE 'Hedge'         END     ) B GROUP BY     b.CURRENCY_OF_MEASURE,     B.CCC_PRODUCT_LINE ORDER BY     ABS(SUM(B.FX_KAPPA)) DESC