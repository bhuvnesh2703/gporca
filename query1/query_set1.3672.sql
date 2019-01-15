SELECT COB_DATE, SUM(LONG_EXPOSURE) AS LONG_EXPOSURE, SUM(SHORT_EXPOSURE) AS SHORT_EXPOSURE, SUM(LONG_EXPOSURE) + SUM(SHORT_EXPOSURE) AS NET_EXPOSURE, SUM(ABS_EXPOSURE) AS ABS_EXPOSURE FROM ( SELECT COB_DATE, CASE WHEN USD_EXPOSURE > 0 THEN SUM(USD_EXPOSURE) ELSE 0 END AS LONG_EXPOSURE, CASE WHEN USD_EXPOSURE < 0 THEN SUM(USD_EXPOSURE) ELSE 0 END AS SHORT_EXPOSURE, SUM(ABS_USD_EXPOSURE) AS ABS_EXPOSURE FROM ( SELECT a.COB_DATE, a.REFERENCE_ENTITY_NAME, a.TAPSCUSIP, sum(coalesce(a.USD_EXPOSURE,0)) as USD_EXPOSURE, abs(sum(coalesce(a.USD_EXPOSURE,0))) as ABS_USD_EXPOSURE FROM cdwuser.U_EXP_MSR a WHERE a.COB_DATE >= '2018-02-20' AND a.CCC_PRODUCT_LINE NOT IN ('DISTRESSED TRADING') AND a.CCC_BANKING_TRADING = 'TRADING' AND a.PRODUCT_TYPE_CODE IN ('BOND','BONDFUT','BONDFUTOPT','BONDIL','BONDOPT','PREF') AND a.GICS_LEVEL_1_NAME = 'FINANCIALS' AND a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME NOT IN ('GAS NATURAL SDG, S.A.','AT SECURITIES B.V.') AND a.FID1_SENIORITY IN ('AT1','SUBT1','SUBUT2') AND a.USD_EXPOSURE <> 0 AND a.book not in ('IGFIN') GROUP BY a.COB_DATE, a.REFERENCE_ENTITY_NAME, a.TAPSCUSIP ) x GROUP BY COB_DATE, USD_EXPOSURE ) y GROUP BY COB_DATE