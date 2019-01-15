WITH cpm_cs_all AS (     SELECT         a.cob_date,         a.ISSUER_COUNTRY_CODE,         a.PRODUCT_TYPE_CODE,         a.PRODUCT_SUB_TYPE_CODE,         a.CURVE_NAME,         CASE             WHEN issuer_country_code = 'JPN' THEN issuer_country_code         ELSE 'NONJPN' END AS CTPY_COUNTRY,         CASE             WHEN curve_name IN ('ms_seccpm', 'cpcrmne') THEN 'MORGAN STANLEY'             WHEN curve_name = 'cpcr_mpefund' THEN 'PEERS'         ELSE 'COUNTERPARTIES' END AS ULT_ISSUER_TYPE,         CASE             WHEN a.PRODUCT_SUB_TYPE_CODE LIKE 'MPE%' THEN 'MPE'             WHEN a.PRODUCT_SUB_TYPE_CODE LIKE 'MNE%' THEN 'MNE'         ELSE 'N/A' END AS SUB_TYPE,         SUM (a.USD_PV01SPRD) AS usd_pv01sprd,         SUM (a.USD_PV10_BENCH) AS usd_pv10_bench     FROM         cdwuser.U_CR_MSR a     WHERE         1 = 1 AND (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and CCC_PL_REPORTING_REGION IN ('JAPAN') AND CCC_TAPS_COMPANY IN ('0302','0347','0853','4043','4298','4863','6120', '6899','6837','6893','4044','5869','0856','6325','0301','0893','0993')         AND a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM') AND         /*CCC_BANKING_TRADING='TRADING' AND*/         (a.VAR_EXCL_FL <> 'Y')     GROUP BY         a.cob_date,         a.ISSUER_COUNTRY_CODE,         a.PRODUCT_TYPE_CODE,         a.PRODUCT_SUB_TYPE_CODE,         a.CURVE_NAME     HAVING NOT(         SUM (a.USD_PV01SPRD) IS NULL AND         SUM (a.USD_PV10_BENCH) IS NULL     ) ), cpm_by_country AS (     SELECT         COB_DATE,         ISSUER_COUNTRY_CODE,         SUM (USD_PV10_BENCH) AS usd_pv10_bench     FROM         cpm_cs_all     GROUP BY         COB_DATE,         ISSUER_COUNTRY_CODE ) SELECT     * FROM     cpm_by_country