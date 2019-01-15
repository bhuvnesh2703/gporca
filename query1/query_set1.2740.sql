SELECT A.COB_DATE, CASE WHEN A.CCC_PRODUCT_LINE IN ('DSP INDEX PRODUCTS TRADING','DSP INDEX PRODUCTS') THEN 'CREDIT-CORPORATES' ELSE A.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, CASE WHEN A.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND A.PRODUCT_TYPE_CODE IN ( 'EQUITY','STOCK','ADR', 'SWAP','WARRANT','WARRNT' ) THEN 'EQUITY_HEDGE' WHEN A.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND A.PRODUCT_TYPE_CODE IN ( 'GVTBOND','ETF','OPTION', 'CRDINDEX','CDSOPTIDX','FUTURE' ) THEN 'OTHER_HEDGE' ELSE 'NOT_HEDGE' END AS HEDGE, SUM(CAST(A.USD_EXPOSURE AS NUMERIC (15,5))) / 1000000 AS USD_NET_EXP FROM CDWUSER.U_DM_FIRMWIDE A WHERE COB_DATE >= '07/01/2016' AND COB_DATE <= '02/28/2018' AND A.VAR_EXCL_FL <> 'Y' AND A.CVA_FL <> 'Y' AND A.VERTICAL_SYSTEM NOT IN ('PIPELINE_NY') AND A.CCC_DIVISION IN ('INT RATE CREDIT CURRENCY', 'FIXED INCOME DIVISION') AND CCC_BUSINESS_AREA NOT IN ( 'ALGEBRIS FUND', 'CMT', 'COMMERCIAL RE (PTG)', 'CORE', 'CREDIT', 'COMMODS FINANCING', 'DERIVATIVE PRODUCTS', 'DSP', 'FID ADMIN', 'DSP - CREDIT', 'FID UNDEFINED', 'FINANCING', 'FIRM_DEBT', 'FUND_SHORT_TERM', 'EM CREDIT TRADING', 'FXEM MACRO TRADING', 'GLOBAL STRUCT PRODUCTS', 'HISTORICAL STRATEGIES', 'INTEREST RATES', 'INVESTMENTS', 'IRCC MANAGEMENT', 'LENDING', 'LEVERAGED LOANS', 'MISCELLANEOUS', 'MORGAN STANLEY INVESTMENT', 'MS CVA MNE - COMMOD', 'MS CVA MNE - FID', 'MSPI INVESTING', 'MUNICIPAL SECURITIES', 'NA ELECTRICITYNATURAL GAS', 'OIL LIQUIDS', 'OLD LANE', 'OTHER', 'OTHER FID', 'OTHER IED', 'OTHERS', 'PRIME BROKERAGE', 'PROCESS DRIVEN TRADING', 'PROP WORKOUT', 'PWM_COMMISSIONS', 'REPO', 'RESIDENTIAL', 'STRUCTURED CREDIT PROD', 'COMMODITIES' ) AND NOT ( CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND ( CCC_PRODUCT_LINE IN ( 'PRIMARY - BONDS','PRIMARY - IG BONDS','PRIMARY - NIG BONDS', 'NON INVSMT GRADE PRIMARY', 'NON IG PRIMARY - LOANS', 'PRIMARY - LOANS', 'NON IG PRIMARY - HY BOND', 'INVESTMENT GRADE PRIMARY' ) OR CCC_STRATEGY IN ('NON IG PRIMARY - HY BOND', 'INVESTMENT GRADE PRIMARY') ) ) AND NOT ( CCC_BUSINESS_AREA IN ('EM CREDIT TRADING') AND ( CCC_PRODUCT_LINE IN ('EM CREDIT PRIMARY') OR CCC_STRATEGY IN ('EM CREDIT PRIMARY') ) ) AND CCC_BUSINESS_AREA NOT LIKE 'CPM%' AND ( CREDIT_TYPE_CD IN ('CREDIT-HY_T1T2') OR A.CCC_PRODUCT_LINE IN ('DISTRESSED TRADING') ) AND NOT ( A.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP' AND SPG_DESC IN ('CMBS IO REREMIC','CMBS IO','CMBS SECURITY','RMBS PRIME RESIDUAL','CMBS INDEX','CORPORATE INDEX') AND CCC_PRODUCT_LINE IN ('CRE LENDING', 'CREL BANK HFI', 'CRE LENDING SEC/HFS') ) GROUP BY A.COB_DATE, CASE WHEN A.CCC_PRODUCT_LINE IN ('DSP INDEX PRODUCTS TRADING','DSP INDEX PRODUCTS') THEN 'CREDIT-CORPORATES' ELSE A.CCC_BUSINESS_AREA END, CASE WHEN A.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND A.PRODUCT_TYPE_CODE IN ( 'EQUITY','STOCK','ADR', 'SWAP','WARRANT','WARRNT' ) THEN 'EQUITY_HEDGE' WHEN A.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND A.PRODUCT_TYPE_CODE IN ( 'GVTBOND','ETF','OPTION', 'CRDINDEX','CDSOPTIDX','FUTURE' ) THEN 'OTHER_HEDGE' ELSE 'NOT_HEDGE' END