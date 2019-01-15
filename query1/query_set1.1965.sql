SELECT COB_DATE, CASE WHEN CURRENCY_OF_MEASURE IN ('USD', 'EUR', 'GBP', 'JPY') THEN CURRENCY_OF_MEASURE ELSE 'OTHERS' END AS CURRENCY_OF_MEASURE1, TERM_NEW_GROUP, SUM (ROUND (USD_IR_UNIFIED_PV01,5)) AS USD_PV01 FROM CDWUSER.U_DM_IR A WHERE COB_DATE IN ( '01/31/2018','02/28/2018') AND CCC_PRODUCT_LINE NOT LIKE 'MS CVA MPE%' AND CCC_BUSINESS_AREA NOT LIKE 'CPM%' AND (CCC_PRODUCT_LINE NOT LIKE 'NON IG PRIMARY%' OR CCC_STRATEGY NOT LIKE 'NON IG PRIMARY%') AND CCC_STRATEGY NOT IN ('MS CVA MPE DERIVS CPM', 'MPE CVA RISK MGMT', 'MPE FVA RISK MGMT', 'MNE CVA RISK MGMT', 'MNE FVA RISK MGMT', 'CORE MPE CVA', 'MONOLINE MPE CVA', 'MS CVA MNE DERIVS FID') AND CCC_BUSINESS_AREA NOT IN ('LENDING') AND CCC_BUSINESS_AREA NOT LIKE '%CVA%' AND VAR_EXCL_FL <> 'Y' AND VERTICAL_SYSTEM NOT IN ('PIPELINE_NY') AND (CCC_PRODUCT_LINE NOT IN ('NON INVSMT GRADE PRIMARY', 'NON IG PRIMARY - LOANS', 'PRIMARY - LOANS', 'NON IG PRIMARY - HY BOND', 'INVESTMENT GRADE PRIMARY') AND CCC_STRATEGY NOT IN ('NON IG PRIMARY - HY BOND', 'INVESTMENT GRADE PRIMARY')) AND CCC_DIVISION IN ('FIXED INCOME DIVISION', 'FID UNDEFINED', 'BANK RESOURCE MANAGEMENT', 'FIC DVA', 'FID DVA', 'COMMODITIES', 'INSTITUTIONAL EQUITY DIVISION') AND NOT (CCC_BUSINESS_AREA IN ('EM CREDIT TRADING') AND (CCC_PRODUCT_LINE IN ('EM CREDIT PRIMARY') OR CCC_STRATEGY IN ('EM CREDIT PRIMARY'))) AND NOT (A.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP' AND PRODUCT_TYPE_CODE = 'WAREHOUSE' AND CCC_BANKING_TRADING_LOCALREG = 'BANKING') AND NOT (A.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP' AND (A.CCC_STRATEGY IN ('NA CREL SEC', 'EU CREL SEC', 'NA CREL BANK HFI') OR A.PRODUCT_TYPE_CODE IN ('CMBS', 'RMBS') OR A.PRODUCT_HIERARCHY_LEVEL7 IN ('CMBS_CMBX_CDS_US', 'CMBS_IO_BOND_US', 'CMBS_TAX_RESID_BOND_US', 'UNKNOWN', 'CMBS_SINGLE_NAME_BOND_US')) AND A.PRODUCT_TYPE_CODE NOT IN ('LOAN', 'MISC', 'SWAP') AND CCC_PRODUCT_LINE IN ('CRE LENDING', 'CREL BANK HFI', 'CRE LENDING SEC/HFS')) AND CCC_DIVISION NOT IN ('FIC DVA','FID DVA') AND VERTICAL_SYSTEM NOT LIKE 'EQUITYSD%' GROUP BY COB_DATE, CASE WHEN CURRENCY_OF_MEASURE IN ('USD', 'EUR', 'GBP', 'JPY') THEN CURRENCY_OF_MEASURE ELSE 'OTHERS' END, TERM_NEW_GROUP ;