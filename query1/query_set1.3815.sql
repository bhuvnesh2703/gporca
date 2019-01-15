SELECT * FROM ( SELECT a.COB_DATE, RANK() OVER( PARTITION BY a.COB_DATE ORDER BY sum(coalesce(a.USD_EXPOSURE,0)) desc) as RANK, a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, sum(coalesce(a.USD_EXPOSURE,0))/1000 as USD_EXPOSURE FROM cdwuser.U_EXP_MSR a WHERE a.COB_DATE >= '2018-02-20' AND a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND a.CCC_BANKING_TRADING = 'TRADING' AND a.CCC_PRODUCT_LINE NOT IN ('DISTRESSED TRADING') AND a.PRODUCT_TYPE_CODE IN ('BOND','BONDFUT','BONDFUTOPT','BONDIL','BONDOPT','PREF') AND (a.GICS_LEVEL_1_NAME <> 'FINANCIALS' OR a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME IN ('GAS NATURAL SDG, S.A.','AT SECURITIES B.V.')) AND a.FID1_SENIORITY IN ('AT1','SUBT1','SUBUT2') AND a.USD_EXPOSURE <> 0 AND a.CCC_TAPS_COMPANY = '0302' GROUP BY a.COB_DATE, a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ) x WHERE RANK = 1