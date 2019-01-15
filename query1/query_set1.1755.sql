SELECT COB_DATE, CCC_DIVISION, CASE WHEN a.PARTIAL_DECOMP_PRODUCT_DESCRIPTION IS NULL THEN 'UNDEFINED' ELSE a.PARTIAL_DECOMP_PRODUCT_DESCRIPTION END PARTIAL_DECOMP_PRODUCT_DESCRIPTION, SUM (a.USD_EQ_PARTIAL_KAPPA) AS USD_EQ_PARTIAL_KAPPA FROM cdwuser.U_EXP_MSR_PARTIAL a WHERE a.cob_date in ('2018-02-28') and ISSUER_COUNTRY_CODE = 'CZE' AND a.CCC_BANKING_TRADING = 'TRADING' AND ABS (a.USD_EQ_PARTIAL_KAPPA) > 0 AND (a.CASH_ISSUE_TYPE IN ('STOCK', 'ADR') OR a.Partial_Decomp_Sector IN ('STOCK', 'ADR')) AND a.ccc_division IN ('FIXED INCOME DIVISION', 'INSTITUTIONAL EQUITY DIVISION') AND ASSET_CLASS='EQ' GROUP BY COB_DATE, CCC_DIVISION, PARTIAL_DECOMP_PRODUCT_DESCRIPTION