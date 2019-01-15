SELECT X.*, RANK() OVER (ORDER BY ABS(USD_NET_EXPOSURE_COB) DESC) AS RANK From ( SELECT COUNTRY, SUM(CASE WHEN IS_COB = 1 THEN COALESCE(USD_NET_EXPOSURE, 0) ELSE 0 END) AS USD_NET_EXPOSURE_COB, SUM(CASE WHEN IS_COB = 1 THEN COALESCE(USD_NET_EXPOSURE, 0) ELSE -COALESCE(USD_NET_EXPOSURE, 0) END) AS USD_NET_EXPOSURE_CHANGE, SUM(CASE WHEN IS_COB = 1 THEN COALESCE(USD_PV10_BENCH, 0) ELSE 0 END) AS USD_PV10_BENCH_COB, SUM(CASE WHEN IS_COB = 1 THEN COALESCE(USD_PV10_BENCH, 0) ELSE -COALESCE(USD_PV10_BENCH, 0) END) AS USD_PV10_BENCH_CHANGE FROM ( SELECT CASE WHEN COB_DATE = '2018-02-28' THEN 1 ELSE 0 END AS IS_COB, COUNTRY_CD_OF_RISK AS COUNTRY, SUM(case when feed_source_name = 'ER1' and PRODUCT_TYPE_CODE IN ('ASCOT', 'CONVRT') and PRODUCT_LEVEL = 'POS' then 0 else usd_exposure end) AS USD_NET_EXPOSURE, sum(case when feed_source_name = 'ER1' and PRODUCT_TYPE_CODE IN ('ASCOT', 'CONVRT') and PRODUCT_LEVEL = 'POS' then usd_credit_pv10pct else usd_pv10_bench end) as USD_PV10_BENCH FROM CDWUSER.u_cr_msr WHERE COB_DATE IN ('2018-02-28', '2018-02-27') AND PARENT_LEGAL_ENTITY = '0517(G)' AND FID1_INDUSTRY_NAME_LEVEL1 = 'SOVEREIGN' AND var_excl_fl <> 'Y' and COUNTRY_CD_OF_RISK in ('ITA','PRT','GRC','ESP','IRL', 'CYP', 'GER', 'FRA', 'GBR', 'DEU') GROUP BY 1,2 ) A GROUP BY COUNTRY ) X