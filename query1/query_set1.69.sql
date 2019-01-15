SELECT     a.COB_DATE,     CASE         WHEN issuer_country_code = 'JPN' THEN a.ISSUER_COUNTRY_CODE     ELSE 'NONJPN' END AS CTPY_COUNTRY,     CASE         WHEN curve_name IN ('ms_seccpm', 'cpcrmne') THEN 'MORGAN STANLEY'         WHEN curve_name = 'cpcr_mpefund' THEN 'PEERS'     ELSE 'COUNTERPARTIES' END AS ULT_ISSUER_TYPE,     CASE         WHEN a.PRODUCT_SUB_TYPE_CODE LIKE 'MPE%' THEN 'MPE'         WHEN a.PRODUCT_SUB_TYPE_CODE LIKE 'MNE%' THEN 'MNE'     ELSE 'N/A' END AS SUB_TYPE,     a.PRODUCT_TYPE_CODE,     SUM (COALESCE (a.USD_PV10_BENCH,0)) AS PV10,     SUM (COALESCE (a.USD_PV01SPRD,0)) AS SPV01 FROM     cdwuser.U_CR_MSR a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-21') and CCC_PL_REPORTING_REGION IN ('JAPAN')     AND  (a.CCC_BUSINESS_AREA IN ('CPM', 'CPM TRADING (MPE)', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR      a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES')) AND     a.ccc_product_line NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE') AND     A.CURVE_TYPE NOT IN ('CPCR_CLEAR') AND     ((a.USD_PV01SPRD IS NOT NULL) or     (a.USD_PV10_BENCH IS NOT NULL)) GROUP BY     a.COB_DATE,     a.PRODUCT_TYPE_CODE,     CASE         WHEN issuer_country_code = 'JPN' THEN a.ISSUER_COUNTRY_CODE     ELSE 'NONJPN' END,     CASE         WHEN curve_name IN ('ms_seccpm', 'cpcrmne') THEN 'MORGAN STANLEY'         WHEN curve_name = 'cpcr_mpefund' THEN 'PEERS'     ELSE 'COUNTERPARTIES' END,     CASE         WHEN a.PRODUCT_SUB_TYPE_CODE LIKE 'MPE%' THEN 'MPE'         WHEN a.PRODUCT_SUB_TYPE_CODE LIKE 'MNE%' THEN 'MNE'     ELSE 'N/A' END