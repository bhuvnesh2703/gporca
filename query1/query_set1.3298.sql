SELECT ccc_product_line, CCC_HIERARCHY_LEVEL9, sectype, BOOK, CCC_PL_REPORTING_REGION, TERM_BUCKET, currency, CALDA_EXCLUSION, PRODUCT_CRITERIA, MS, sum(case when is_cob = 1 then usd_pv01 else 0 end) as usd_pv01_cob, sum(case when is_cob = 1 then usd_pv01 else -usd_pv01 end) as usd_pv01_change, sum(case when is_cob = 1 then usd_fx else 0 end) as usd_fx_cob, sum(case when is_cob = 1 then usd_fx else -usd_fx end) as usd_fx_change, sum(case when is_cob = 1 then usd_ir_gamma else 0 end) as usd_ir_gamma_cob, sum(case when is_cob = 1 then usd_ir_gamma else -usd_ir_gamma end) as usd_ir_gamma_change, sum(case when is_cob = 1 then usd_pv01sprd else 0 end) as usd_pv01sprd_cob, sum(case when is_cob = 1 then usd_pv01sprd else -usd_pv01sprd end) as usd_pv01sprd_change, sum(case when is_cob = 1 then usd_cm_delta else 0 end) as usd_cm_delta_cob, sum(case when is_cob = 1 then usd_cm_delta else -usd_cm_delta end) as usd_cm_delta_change, sum(case when is_cob = 1 then nramv else 0 end) as nramv_cob, sum(case when is_cob = 1 then nramv else -nramv end) as nramv_change, sum(case when is_cob = 1 then USD_FX_KAPPA else 0 end) as USD_FX_KAPPA_cob, sum(case when is_cob = 1 then USD_FX_KAPPA else -USD_FX_KAPPA end) as USD_FX_KAPPA_change, sum(case when is_cob = 1 then USD_CM_KAPPA else 0 end) as USD_CM_KAPPA_cob, sum(case when is_cob = 1 then USD_CM_KAPPA else -USD_CM_KAPPA end) as USD_CM_KAPPA_change, sum(case when is_cob = 1 then USD_EQ_KAPPA else 0 end) as USD_EQ_KAPPA_cob, sum(case when is_cob = 1 then USD_EQ_KAPPA else -USD_EQ_KAPPA end) as USD_EQ_KAPPA_change, sum(case when is_cob = 1 then USD_KAPPA else 0 end) as USD_KAPPA_cob, sum(case when is_cob = 1 then USD_KAPPA else -USD_KAPPA end) as USD_KAPPA_change, sum(case when is_cob = 1 then USD_CR_KAPPA else 0 end) as USD_CR_KAPPA_cob, sum(case when is_cob = 1 then USD_CR_KAPPA else -USD_CR_KAPPA end) as USD_CR_KAPPA_change, sum(case when is_cob = 1 then PV10_BENCH else 0 end) as USD_PV10_BENCH_cob, sum(case when is_cob = 1 then PV10_BENCH else -PV10_BENCH end) as USD_PV10_BENCH_change, sum(case when is_cob = 1 then USD_NOTIONAL else 0 end) as USD_NOTIONAL_cob, sum(case when is_cob = 1 then USD_NOTIONAL else -USD_NOTIONAL end) as USD_NOTIONAL_change, sum(case when is_cob = 1 then USD_MARKET_VALUE else 0 end) as USD_MARKET_VALUE_cob, sum(case when is_cob = 1 then USD_MARKET_VALUE else -USD_MARKET_VALUE end) as USD_MARKET_VALUE_change FROM ( SELECT ccc_product_line, BOOK, CCC_HIERARCHY_LEVEL9, CCC_PL_REPORTING_REGION, case when 12*TERM_OF_MEASURE/365 < 6.5 then '6m' when 12*TERM_OF_MEASURE/365 < 18 then '1y' when 12*TERM_OF_MEASURE/365 < 30 then '2y' when 12*TERM_OF_MEASURE/365 < 42 then '3y' when 12*TERM_OF_MEASURE/365 < 54 then '4y' when 12*TERM_OF_MEASURE/365 < 66 then '5y' when 12*TERM_OF_MEASURE/365 < 90 then '7y' when 12*TERM_OF_MEASURE/365 < 126 then '10y' when 12*TERM_OF_MEASURE/365 >= 126 then '+10y' end TERM_BUCKET, currency, CALDA_EXCLUSION, PRODUCT_CRITERIA, MS, case when cob_date = '2018-02-28' then 1 else 0 end as is_cob, case when product_type_code IN ('BOND', 'BONDIL', 'FRN') THEN 'EXPOSURE' ELSE 'HEDGE' END AS sectype, sum(usd_pv01) as usd_pv01, sum(usd_fx) as usd_fx, sum(usd_ir_gamma) as usd_ir_gamma, sum(usd_pv01sprd) as usd_pv01sprd, sum(cm_delta) as usd_cm_delta, sum(nramv) as nramv, sum(USD_FX_KAPPA) as USD_FX_KAPPA, sum(USD_CM_KAPPA) as USD_CM_KAPPA, sum(USD_EQ_KAPPA) as USD_EQ_KAPPA, sum(USD_KAPPA) as USD_KAPPA, sum(USD_CR_KAPPA) as USD_CR_KAPPA, sum(PV10_BENCH) as PV10_BENCH, sum(USD_NOTIONAL) AS USD_NOTIONAL, SUM(USD_MARKET_VALUE) AS USD_MARKET_VALUE FROM ( SELECT ccc_product_line, CCC_HIERARCHY_LEVEL9, product_type_code, cob_date, BOOK, CCC_PL_REPORTING_REGION, TERM_OF_MEASURE, case when CURRENCY_OF_MEASURE = 'EUR' then 'EUR' else 'USD AND OTHER' end as currency, case when ccc_BOOK_detail like '%FRN%' AND ccc_division IN ('FIC DVA', 'FID DVA') then 'EXCLUDE' else 'INCLUDE' end as CALDA_EXCLUSION, CASE WHEN CCC_BOOK_DETAIL LIKE '%FRN%' THEN 'FRN' WHEN CCC_BOOK_DETAIL LIKE '%BOND%' THEN 'BOND' WHEN CCC_BOOK_DETAIL LIKE '%SWAP%' THEN 'SWAP' WHEN CCC_BOOK_DETAIL LIKE '%LOAN%' THEN 'BOND' ELSE 'OTHER' END AS PRODUCT_CRITERIA, case when POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'MORGAN STANLEY' then 'MS' else 'Non-MS' end as MS, SUM(usd_ir_unified_pv01) as usd_pv01, SUM(case when CURRENCY_OF_MEASURE not in ('UBD', 'USD') then usd_fx else 0 end) as usd_fx, SUM(case when FEED_SOURCE_NAME NOT IN ('CORISK', 'ER1') then coalesce(USD_CM_DELTA, 0) WHEN FEED_SOURCE_NAME = 'CORISK' AND PRODUCT_TYPE_CODE not in ('CASH','MISC','ERROR','TBD','INTEREST RATE','CURRENCY','EQUITY INDEX','CREDIT') THEN coalesce(USD_CM_DELTA, 0) else 0 end) as cm_delta, SUM(case when FEED_SOURCE_NAME NOT IN ('CORISK', 'ER1') then coalesce(USD_DELTA, 0) when FEED_SOURCE_NAME IN ('CORISK') AND PRODUCT_TYPE_CODE = 'EQUITY INDEX' then coalesce(USD_CM_DELTA, 0) else 0 end) as nramv, SUM(coalesce(usd_ir_kappa/10,0)) + sum(coalesce(usd_real_kappa/10,0)) as usd_kappa, SUM(case when FEED_SOURCE_NAME <> 'CORISK' then coalesce(USD_FX_KAPPA, 0) + coalesce(usd_fx_partial_kappa, 0) when FEED_SOURCE_NAME = 'CORISK' AND PRODUCT_TYPE_CODE = 'CURRENCY' then coalesce(LCY_FX_KAPPA, 0) /1000 else 0 end) as usd_fx_kappa, SUM(case when product_type_code not in (upper('Interest Rate'),upper('euro pwr spread'),upper('euro gas spread'),upper('timespread'),upper('TBD')) then (CASE WHEN feed_source_name <> 'CORISK' then usd_cm_kappa ELSE CASE WHEN product_type_code=upper('Eur ng') then coalesce(raw_cm_kappa/10,0) /1000 WHEN product_type_code NOT IN(upper('equity index'),upper('Currency'), upper('Credit')) THEN coalesce(raw_cm_kappa,0) /1000 ELSE 0 end END) end) as usd_cm_kappa, SUM(coalesce(usd_eq_kappa,0)+(CASE WHEN FEED_SOURCE_NAME = 'CORISK' AND PRODUCT_TYPE_CODE = 'EQUITY INDEX' THEN coalesce(raw_cm_kappa, 0) ELSE 0 END)) AS usd_eq_kappa, SUM(usd_ir_partial_gamma) as usd_ir_gamma, SUM(case when POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'MORGAN STANLEY' then usd_pv01sprd else 0 end) AS usd_pv01sprd, SUM(case when POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'MORGAN STANLEY' then coalesce(usd_cr_kappa,0) else 0 end) AS usd_cr_kappa, SUM(case when FEED_SOURCE_NAME NOT IN ('ER1') and POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'MORGAN STANLEY' then usd_pv10_bench WHEN FEED_SOURCE_NAME = 'ER1' AND PRODUCT_TYPE_CODE IN ('ASCOT','CONVRT') and POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'MORGAN STANLEY' THEN coalesce(USD_CREDIT_PV10PCT,0) else 0 end) AS PV10_BENCH, SUM(usd_notional) usd_notional, SUM(USD_MARKET_VALUE) as USD_MARKET_VALUE FROM cdwuser.U_EXP_MSR WHERE vertical_system NOT LIKE ('PIPELINE%') AND cob_date in ('2018-02-28', '2018-01-31') AND ccc_division IN ('FIC DVA', 'FID DVA') AND var_excl_fl <> 'Y' GROUP BY ccc_product_line, CCC_HIERARCHY_LEVEL9, product_type_code, cob_date, BOOK, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PL_REPORTING_REGION, TERM_OF_MEASURE, case when CURRENCY_OF_MEASURE = 'EUR' then 'EUR' else 'USD AND OTHER' end, case when ccc_BOOK_detail like '%FRN%' AND ccc_division IN ('FIC DVA', 'FID DVA') then 'EXCLUDE' else 'INCLUDE' end, CASE WHEN CCC_BOOK_DETAIL LIKE '%FRN%' THEN 'FRN' WHEN CCC_BOOK_DETAIL LIKE '%BOND%' THEN 'BOND' WHEN CCC_BOOK_DETAIL LIKE '%SWAP%' THEN 'SWAP' WHEN CCC_BOOK_DETAIL LIKE '%LOAN%' THEN 'BOND' ELSE 'OTHER' END, case when POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'MORGAN STANLEY' then 'MS' else 'Non-MS' end union all SELECT ccc_product_line, CCC_HIERARCHY_LEVEL9, product_type_code_decomp, cob_date, BOOK, CCC_PL_REPORTING_REGION, 0 as TERM_OF_MEASURE, '' as currency, case when ccc_BOOK_detail like '%FRN%' AND ccc_division IN ('FIC DVA', 'FID DVA') then 'EXCLUDE' else 'INCLUDE' end as CALDA_EXCLUSION, CASE WHEN CCC_BOOK_DETAIL LIKE '%FRN%' THEN 'FRN' WHEN CCC_BOOK_DETAIL LIKE '%BOND%' THEN 'BOND' WHEN CCC_BOOK_DETAIL LIKE '%SWAP%' THEN 'SWAP' WHEN CCC_BOOK_DETAIL LIKE '%LOAN%' THEN 'BOND' ELSE 'OTHER' END AS PRODUCT_CRITERIA, case when ISSUER_PARTY_DARWIN_NAME = 'MORGAN STANLEY' then 'MS' else 'Non-MS' end as MS, 0 AS USD_PV01, 0 as usd_fx, sum(case when (COALESCE (PRODUCT_TYPE_CODE_DECOMP, '') = 'COMM' OR COALESCE (CASH_ISSUE_TYPE, '') = 'COMM') then coalesce(USD_CM_DELTA_DECOMP, 0) else 0 end) cm_delta, SUM(case when COALESCE (PRODUCT_TYPE_CODE_DECOMP, '') <> 'COMM' AND COALESCE (CASH_ISSUE_TYPE, '') <> 'COMM' then coalesce(USD_EQ_DELTA_DECOMP, 0) else 0 end) as nramv, 0 as usd_kappa, 0 AS usd_fx_kappa, 0 as usd_cm_kappa, 0 as usd_eq_kappa, 0 as usd_ir_gamma, 0 as usd_pv01sprd, 0 as usd_cr_kappa, 0 as PV10_BENCH, 0 AS USD_NOTIONAL, 0 AS USD_MARKET_VALUE FROM cdwuser.u_decomp_msr WHERE cob_date in ('2018-02-28', '2018-01-31') AND vertical_system NOT LIKE ('PIPELINE%') AND var_excl_fl <> 'Y' AND FEED_SOURCE_ID = 301 AND ccc_division IN ('FIC DVA', 'FID DVA') GROUP BY ccc_product_line, CCC_HIERARCHY_LEVEL9, product_type_code_decomp, cob_date, BOOK, CCC_PL_REPORTING_REGION, TERM_OF_MEASURE, case when ccc_BOOK_detail like '%FRN%' AND ccc_division IN ('FIC DVA', 'FID DVA') then 'EXCLUDE' else 'INCLUDE' end, CASE WHEN CCC_BOOK_DETAIL LIKE '%FRN%' THEN 'FRN' WHEN CCC_BOOK_DETAIL LIKE '%BOND%' THEN 'BOND' WHEN CCC_BOOK_DETAIL LIKE '%SWAP%' THEN 'SWAP' WHEN CCC_BOOK_DETAIL LIKE '%LOAN%' THEN 'BOND' ELSE 'OTHER' END, case when ISSUER_PARTY_DARWIN_NAME = 'MORGAN STANLEY' then 'MS' else 'Non-MS' end ) a GROUP BY ccc_product_line, BOOK, CCC_HIERARCHY_LEVEL9, CCC_PL_REPORTING_REGION, TERM_OF_MEASURE, currency, CALDA_EXCLUSION, PRODUCT_CRITERIA, MS, case when product_type_code IN ('BOND', 'BONDIL', 'FRN') THEN 'EXPOSURE' ELSE 'HEDGE' END, case when cob_date = '2018-02-28' then 1 else 0 end ) B GROUP BY ccc_product_line, CCC_HIERARCHY_LEVEL9, sectype, BOOK, CCC_PL_REPORTING_REGION, TERM_BUCKET, currency, CALDA_EXCLUSION, PRODUCT_CRITERIA, MS