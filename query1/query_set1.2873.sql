SELECT b.COB_DATE, b.CCC_BUSINESS_AREA, b.CCC_PRODUCT_LINE, b.RISK_SYSTEM, b.PRODUCT_TYPE, b.SCENARIO_TYPE, b.IS_INCLUDED, b.STRESS_SCENARIO, b.EXCLUDE_CODE, b.RUN_PROFILE, b.CCC_STRATEGY, b.SCENARIO_DIMENSION, b.CCC_BANKING_TRADING, b.CCC_DIVISION, b.subprod_product_type, b.subprod_product_subtype, b.Include_in_reg_cap, b.attribution, b.executive_model, strategy_details, has_crossgamma, b.book, CASE WHEN ( (has_crossgamma = 'Y') AND b.ATTRIBUTION = 'CM GAMMA' ) THEN 'excluded' ELSE b.IS_INCLUDED END AS INCLUDE, product_sub_type_name, Sum(CASE WHEN scenario_type = 'GREEK' AND attribution = 'CM DELTA' THEN scenario_pnl ELSE 0 END) AS delta_PNL, Sum(CASE WHEN is_included = 'YES' THEN scenario_pnl ELSE 0 END) AS SCENARIO_PNL, CASE WHEN b.BOOK IN ('1928', '2203', '2204', '2257', '2267', '2353', '2357', '2363', '2365', '2367', '2369', '2403', '3296', '3297', '2269', '2273', '2277', '2278', '2279', '2282', '2293', '2298', '2303', '2309', '2334', '2335', '2336', '2337', '2380', '2398', '2713', '2714', '2715', '2716', '2721', '2722', '2723', '2724', '2725', '2735', '2746', '12026', '12027', '12028', '12029') THEN 'FlexDeal' WHEN b.Book IN ('1927', '1982', '2011', '2202', '2352', '2356', '2368', '2825', '2270', '2280', '2294', '2296', '2302', '2308', '2322', '2325', '2332', '2345', '2518', '2739', '2740', '2741', '2742', '2743', '2744') THEN 'StructHedge' ELSE 'Other' END AS STRUCTSPLIT FROM DWUSER.U_MODULAR_SCENARIOS b WHERE COB_DATE in ('2018-02-28') and Run_profile IN ('CM_MOD_SCN_RUN') AND Stress_Scenario IN ('CM_PRICE_DOWN50_VOL_0', 'CM_PRICE_DOWN40_VOL_0', 'CM_PRICE_DOWN30_VOL_0', 'CM_PRICE_DOWN20_VOL_0', 'CM_PRICE_DOWN10_VOL_0', 'CM_PRICE_0_VOL_0', 'CM_PRICE_UP10_VOL_0', 'CM_PRICE_UP20_VOL_0', 'CM_PRICE_UP30_VOL_0', 'CM_PRICE_UP40_VOL_0', 'CM_PRICE_UP50_VOL_0') AND b.ccc_business_area = 'COMMODITIES' AND b.ccc_product_line IN ('COMMOD EXOTICS', 'COMMOD INDEX') AND ( b.SUBPROD_PRODUCT_TYPE NOT IN ('CURRENCY', 'CREDIT', 'INTEREST RATE', 'WEATHER') OR b.SUBPROD_PRODUCT_TYPE IS NULL ) AND ( NOT ( ccc_strategy IN ('CVA RISK MANAGEMENT', 'CMD LEGACY LOANS & CLAIMS') OR ccc_product_line IN ('COMMOD - FUNDING', 'COMMOD LENDING') ) ) GROUP BY b.COB_DATE, b.CCC_BUSINESS_AREA, b.CCC_PRODUCT_LINE, b.RISK_SYSTEM, b.PRODUCT_TYPE, b.SCENARIO_TYPE, b.IS_INCLUDED, b.STRESS_SCENARIO, b.EXCLUDE_CODE, b.RUN_PROFILE, b.CCC_STRATEGY, b.SCENARIO_DIMENSION, b.CCC_BANKING_TRADING, b.CCC_DIVISION, b.subprod_product_type, b.subprod_product_subtype, b.Include_in_reg_cap, b.attribution, product_sub_type_name, b.executive_model, strategy_details, has_crossgamma, b.Book