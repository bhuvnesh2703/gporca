SELECT * FROM ( SELECT CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (IR_PV01, 0) ELSE 0 END) AS IR_PV01_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (IR_PV01, 0) ELSE - COALESCE (IR_PV01, 0) END) AS IR_PV01_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (IR_VEGA, 0) ELSE 0 END) AS IR_VEGA_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (IR_VEGA, 0) ELSE - COALESCE (IR_VEGA, 0) END) AS IR_VEGA_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (IR_GAMMA, 0) ELSE 0 END) AS IR_GAMMA_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (IR_GAMMA, 0) ELSE - COALESCE (IR_GAMMA, 0) END) AS IR_GAMMA_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_PV01SPRD, 0) ELSE 0 END) AS USD_PV01SPRD_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_PV01SPRD, 0) ELSE - COALESCE (USD_PV01SPRD, 0) END) AS USD_PV01SPRD_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_PV10_BENCH, 0) ELSE 0 END) AS USD_PV10_BENCH_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_PV10_BENCH, 0) ELSE - COALESCE (USD_PV10_BENCH, 0) END) AS USD_PV10_BENCH_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_CR_KAPPA, 0) ELSE 0 END) AS USD_CR_KAPPA_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_CR_KAPPA, 0) ELSE - COALESCE (USD_CR_KAPPA, 0) END) AS USD_CR_KAPPA_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_FX, 0) ELSE 0 END) AS USD_FX_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_FX, 0) ELSE - COALESCE (USD_FX, 0) END) AS USD_FX_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_FX_KAPPA, 0) ELSE 0 END) AS USD_FX_KAPPA_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_FX_KAPPA, 0) ELSE - COALESCE (USD_FX_KAPPA, 0) END) AS USD_FX_KAPPA_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (NRAMV, 0) ELSE 0 END) AS NRAMV_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (NRAMV, 0) ELSE - COALESCE (NRAMV, 0) END) AS NRAMV_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_EQ_KAPPA, 0) ELSE 0 END) AS USD_EQ_KAPPA_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_EQ_KAPPA, 0) ELSE - COALESCE (USD_EQ_KAPPA, 0) END) AS USD_EQ_KAPPA_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (CM_DELTA, 0) ELSE 0 END) AS CM_DELTA_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (CM_DELTA, 0) ELSE - COALESCE (CM_DELTA, 0) END) AS CM_DELTA_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_PROCEED, 0) ELSE 0 END) AS USD_PROCEED_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_PROCEED, 0) ELSE - COALESCE (USD_PROCEED, 0) END) AS USD_PROCEED_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_NET_EXPOSURE, 0) ELSE 0 END) AS USD_NET_EXPOSURE_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (USD_NET_EXPOSURE, 0) ELSE - COALESCE (USD_NET_EXPOSURE, 0) END) AS USD_NET_EXPOSURE_CHANGE, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (usd_pv10_combined, 0) ELSE 0 END) AS usd_pv10_combined_COB, SUM (CASE WHEN IS_COB = 1 THEN COALESCE (usd_pv10_combined, 0) ELSE - COALESCE (usd_pv10_combined, 0) END) AS usd_pv10_combined_CHANGE FROM ( SELECT CASE WHEN COB_DATE = '2018-02-28' THEN 1 ELSE 0 END AS IS_COB, CASE WHEN CCC_BUSINESS_AREA NOT IN ('CPM', 'CPM TRADING (MPE)', 'MS CVA MNE - FID') AND CCC_DIVISION = 'FIXED INCOME DIVISION' THEN 'FIXED INCOME DIVISION (EX-CVA)' ELSE CCC_DIVISION END AS CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, SUM (USD_PV01) AS IR_PV01, SUM (USD_KAPPA) AS IR_VEGA, SUM (USD_IR_PARTIAL_GAMMA) AS IR_GAMMA, SUM (USD_PV01SPRD) AS USD_PV01SPRD, SUM (USD_PV10_BENCH) AS USD_PV10_BENCH, SUM (USD_CR_KAPPA) AS USD_CR_KAPPA, SUM (USD_FX) AS USD_FX, SUM (USD_FX_KAPPA) AS USD_FX_KAPPA, SUM (NRAMV) AS NRAMV, SUM (USD_EQ_KAPPA) AS USD_EQ_KAPPA, SUM (CM_DELTA) AS CM_DELTA, SUM (USD_MARKET_VALUE) AS USD_PROCEED, SUM (USD_EXPOSURE) AS USD_NET_EXPOSURE, SUM (USD_PV10_COMBINED) AS USD_PV10_COMBINED FROM ( SELECT COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, 0 AS USD_PV01, 0 AS USD_FX, SUM (CASE WHEN (COALESCE (PRODUCT_TYPE_CODE_DECOMP, '') = 'COMM' OR COALESCE (CASH_ISSUE_TYPE, '') = 'COMM') AND FEED_SOURCE_ID = 301 THEN COALESCE (USD_CM_DELTA_DECOMP, 0) ELSE 0 END) AS CM_DELTA, SUM (CASE WHEN (COALESCE (PRODUCT_TYPE_CODE_DECOMP, '') <> 'COMM' OR COALESCE (CASH_ISSUE_TYPE, '') <> 'COMM') AND FEED_SOURCE_ID = 301 THEN COALESCE (USD_EQ_DELTA_DECOMP, 0) ELSE 0 END) AS NRAMV, 0 AS USD_KAPPA, 0 AS USD_CR_KAPPA, 0 AS USD_EQ_KAPPA, 0 AS USD_FX_KAPPA, 0 AS USD_IR_PARTIAL_GAMMA, 0 AS USD_PV01SPRD, 0 AS USD_EXPOSURE, 0 AS USD_MARKET_VALUE, 0 AS USD_PV10_BENCH, 0 AS USD_PV10_COMBINED FROM CDWUSER.U_DECOMP_MSR WHERE COB_DATE IN ('2018-02-28','2018-02-27') AND VAR_EXCL_FL <> 'Y' AND PARENT_LEGAL_ENTITY = '0517(G)' AND ( ( CCC_BANKING_TRADING = 'TRADING' ) OR ( CCC_BANKING_TRADING = 'BANKING' AND (CCC_BUSINESS_AREA IN ('CPM','CREDIT','MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING'))) ) GROUP BY COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE UNION ALL SELECT COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, SUM (USD_PV01) AS USD_PV01, SUM (USD_FX) AS USD_FX, SUM (CM_DELTA) AS CM_DELTA, SUM (NRAMV) AS NRAMV, SUM (USD_KAPPA) AS USD_KAPPA, SUM (USD_CR_KAPPA) AS USD_CR_KAPPA, SUM (USD_EQ_KAPPA) AS USD_EQ_KAPPA, SUM (USD_FX_KAPPA) AS USD_FX_KAPPA, SUM (USD_IR_PARTIAL_GAMMA) AS USD_IR_PARTIAL_GAMMA, SUM (USD_PV01SPRD) AS USD_PV01SPRD, SUM (USD_EXPOSURE) AS USD_EXPOSURE, SUM (USD_MARKET_VALUE) AS USD_MARKET_VALUE, SUM (USD_PV10_BENCH) AS USD_PV10_BENCH, SUM ( CASE WHEN PRODUCT_TYPE_CODE IN ('BONDFUT', 'CASH', 'REPO') OR CCC_STRATEGY = 'MS DVA - STR NOTES' THEN 0 WHEN (PRODUCT_TYPE_CODE = 'DISTRESSED TRADING' AND PRODUCT_TYPE_CODE = 'TRRSWAP') THEN USD_PV10_BENCH ELSE (COALESCE (USD_PV10_BENCH, 0) + COALESCE (USD_PV10_BENCH_IMPLIED, 0)) END ) AS USD_PV10_COMBINED FROM ( SELECT COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, CCC_STRATEGY, PRODUCT_TYPE_CODE, SUM (USD_PV01) AS USD_PV01, SUM (USD_FX) AS USD_FX, SUM (CM_DELTA) AS CM_DELTA, SUM (NRAMV) AS NRAMV, SUM (USD_KAPPA) AS USD_KAPPA, SUM (USD_CR_KAPPA) AS USD_CR_KAPPA, SUM (USD_EQ_KAPPA) AS USD_EQ_KAPPA, SUM (USD_FX_KAPPA) AS USD_FX_KAPPA, SUM (USD_IR_PARTIAL_GAMMA) AS USD_IR_PARTIAL_GAMMA, SUM (USD_PV01SPRD) AS USD_PV01SPRD, SUM (USD_EXPOSURE) AS USD_EXPOSURE, SUM (USD_MARKET_VALUE) AS USD_MARKET_VALUE, SUM (USD_PV10_BENCH) AS USD_PV10_BENCH, SUM (CASE WHEN ((VAR_EXCL_FL = 'N' AND VERTICAL_SYSTEM NOT LIKE 'STS_%') OR (VAR_EXCL_FL = 'N' AND PRODUCT_TYPE_CODE IN ('SWAPIL')) OR (USD_PV10_BENCH IS NOT NULL)) THEN 0 ELSE (COALESCE (CASE WHEN FEED_SOURCE_NAME <> 'ER1' THEN USD_PV01SPRD ELSE 0 END, 0) * 0.1 * CREDIT_SPREAD) END) AS USD_PV10_BENCH_IMPLIED FROM ( SELECT COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, CCC_STRATEGY, PRODUCT_TYPE_CODE, VAR_EXCL_FL, VERTICAL_SYSTEM, FEED_SOURCE_NAME, COALESCE (USD_IR_UNIFIED_PV01, 0) AS USD_PV01, CASE WHEN CURRENCY_OF_MEASURE IN ('UBD', 'USD') THEN 0 ELSE COALESCE (USD_FX, 0) END AS USD_FX, CASE WHEN FEED_SOURCE_NAME NOT IN ('CORISK', 'ER1') THEN COALESCE (USD_CM_DELTA, 0) WHEN FEED_SOURCE_NAME = 'CORISK' AND PRODUCT_TYPE_CODE NOT IN ('CASH', 'MISC', 'ERROR', 'TBD', 'INTEREST RATE', 'CURRENCY') AND PRODUCT_TYPE_CODE NOT IN ('EQUITY INDEX', 'CREDIT') THEN COALESCE (USD_CM_DELTA, 0) ELSE 0 END AS CM_DELTA, CASE WHEN FEED_SOURCE_NAME NOT IN ('CORISK', 'ER1') THEN COALESCE (USD_DELTA, 0) WHEN FEED_SOURCE_NAME = 'CORISK' AND PRODUCT_TYPE_CODE = 'EQUITY INDEX' THEN COALESCE (USD_DELTA, 0) ELSE 0 END AS NRAMV, COALESCE (USD_IR_KAPPA, 0) / 10.0 AS USD_KAPPA, COALESCE (USD_CR_KAPPA, 0) AS USD_CR_KAPPA, COALESCE (CASE WHEN FEED_SOURCE_NAME = 'CORISK' AND PRODUCT_TYPE_CODE = 'EQUITY INDEX' THEN COALESCE (RAW_CM_KAPPA, 0) / 1000 ELSE USD_EQ_KAPPA END, 0) AS USD_EQ_KAPPA, CASE WHEN FEED_SOURCE_NAME <> 'CORISK' THEN COALESCE (USD_FX_KAPPA, 0) + COALESCE ( USD_FX_PARTIAL_KAPPA, 0) WHEN FEED_SOURCE_NAME = 'CORISK' AND PRODUCT_TYPE_CODE = 'CURRENCY' THEN COALESCE (LCY_FX_KAPPA, 0) / 1000 ELSE 0 END AS USD_FX_KAPPA, COALESCE (USD_IR_PARTIAL_GAMMA, 0) AS USD_IR_PARTIAL_GAMMA, COALESCE (USD_PV01SPRD, 0) USD_PV01SPRD, COALESCE (USD_EXPOSURE, 0) AS USD_EXPOSURE, COALESCE (USD_MARKET_VALUE, 0) AS USD_MARKET_VALUE, CASE WHEN FEED_SOURCE_NAME <> 'ER1' THEN USD_PV10_BENCH WHEN (FEED_SOURCE_NAME = 'ER1' AND PRODUCT_TYPE_CODE IN ('ASCOT', 'CONVRT')) THEN USD_CREDIT_PV10PCT ELSE 0 END AS USD_PV10_BENCH, CASE WHEN FEED_SOURCE_NAME <> 'ER1' THEN CREDIT_SPREAD ELSE 0 END AS CREDIT_SPREAD FROM CDWUSER.U_EXP_MSR a WHERE COB_DATE IN ('2018-02-28','2018-02-27') AND VAR_EXCL_FL <> 'Y' AND PARENT_LEGAL_ENTITY = '0517(G)' AND ( ( CCC_BANKING_TRADING = 'TRADING' ) OR ( CCC_BANKING_TRADING = 'BANKING' AND (CCC_BUSINESS_AREA IN ('CPM','CREDIT','MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) ) ))XX GROUP BY COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, CCC_STRATEGY, PRODUCT_TYPE_CODE ) YY GROUP BY COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE ) ZZ GROUP BY IS_COB, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE ) AA GROUP BY CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE ) BB WHERE IR_PV01_COB + IR_PV01_CHANGE + IR_VEGA_COB + IR_VEGA_CHANGE + IR_GAMMA_COB + IR_GAMMA_CHANGE + USD_PV01SPRD_COB + USD_PV01SPRD_CHANGE + USD_PV10_BENCH_COB + USD_PV10_BENCH_CHANGE + USD_CR_KAPPA_COB + USD_CR_KAPPA_CHANGE + USD_FX_COB + USD_FX_CHANGE + NRAMV_COB + NRAMV_CHANGE + USD_EQ_KAPPA_COB + USD_EQ_KAPPA_CHANGE + CM_DELTA_COB + CM_DELTA_CHANGE + USD_PROCEED_COB + USD_PROCEED_CHANGE + USD_NET_EXPOSURE_COB + USD_NET_EXPOSURE_CHANGE + usd_pv10_combined_COB + usd_pv10_combined_change <> 0