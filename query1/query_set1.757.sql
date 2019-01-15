SELECT * FROM ( SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME ,SUM(a.USD_MARKET_VALUE) AS MPE_CVA ,0 AS MNE_CVA ,0 AS MPE_BPV10 ,0 AS MNE_BPV10 ,0 AS CVA_EXPOSURE ,0 AS CVA_JTD ,0 AS Hedge_BPV10 ,0 AS Hedge_EXPOSURE ,0 AS Hedge_JTD ,0 AS MPE_FVA ,0 AS MPE_FVA_BPV10 ,0 AS MNE_FVA ,0 AS MNE_FVA_BPV10 ,0 AS FVA_EXPOSURE ,0 AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE  (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and    (a.CCC_BUSINESS_AREA IN ( 'CPM' ,'CPM TRADING (MPE)' ,'CREDIT' ,'MS CVA MNE - FID' ,'MS CVA MNE - COMMOD' ) OR a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING' ) ) AND a.PRODUCT_SUB_TYPE_CODE IN ( 'MPE' ,'MPE_CVA' ,'MPE_PROXY' ,'MNE_CP' ) AND ( a.USD_MARKET_VALUE IS NOT NULL OR a.USD_PV10_BENCH IS NOT NULL ) AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME   UNION ALL  SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME ,0 AS MPE_CVA ,0 AS MNE_CVA ,SUM(a.USD_PV10_BENCH) AS MPE_BPV10 ,0 AS MNE_BPV10 ,0 AS CVA_EXPOSURE ,0 AS CVA_JTD ,0 AS Hedge_BPV10 ,0 AS Hedge_EXPOSURE ,0 AS Hedge_JTD ,0 AS MPE_FVA ,0 AS MPE_FVA_BPV10 ,0 AS MNE_FVA ,0 AS MNE_FVA_BPV10 ,0 AS FVA_EXPOSURE ,0 AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE  (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and    (a.CCC_BUSINESS_AREA IN ( 'CPM' ,'CPM TRADING (MPE)' ,'CREDIT' ,'MS CVA MNE - FID' ,'MS CVA MNE - COMMOD' ) OR a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING' ) ) AND a.PRODUCT_SUB_TYPE_CODE IN ( 'MPE' ,'MPE_CVA' ,'MPE_PROXY' ,'MNE_CP' ) AND ( a.USD_MARKET_VALUE IS NOT NULL OR a.USD_PV10_BENCH IS NOT NULL ) AND A.CURVE_NAME NOT IN ('cpcr_mpefund') AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME   UNION ALL  SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME ,0 AS MPE_CVA ,0 AS MNE_CVA ,0 AS MPE_CVA_BPV10 ,0 AS MNE_BPV10 ,0 AS CVA_EXPOSURE ,0 AS CVA_JTD ,0 AS Hedge_BPV10 ,0 AS Hedge_EXPOSURE ,0 AS Hedge_JTD ,SUM(a.USD_MARKET_VALUE) AS MPE_FVA ,0 AS MPE_FVA_BPV10 ,0 AS MNE_FVA ,0 AS MNE_FVA_BPV10 ,0 AS FVA_EXPOSURE ,0 AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and    (a.CCC_BUSINESS_AREA IN ( 'CPM' ,'CPM TRADING (MPE)' ,'CREDIT' ,'MS CVA MNE - FID' ,'MS CVA MNE - COMMOD' ) OR a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING' ) ) AND a.PRODUCT_SUB_TYPE_CODE IN ( 'MPE_FVA_PROXY' ,'MPE_FVA' ,'MPE_FVA_RAW' ) AND ( a.USD_MARKET_VALUE IS NOT NULL OR a.USD_PV10_BENCH IS NOT NULL ) AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME   UNION ALL  SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME ,0 AS MPE_CVA ,0 AS MNE_CVA ,0 AS MPE_CVA_BPV10 ,0 AS MNE_BPV10 ,0 AS CVA_EXPOSURE ,0 AS CVA_JTD ,0 AS Hedge_BPV10 ,0 AS Hedge_EXPOSURE ,0 AS Hedge_JTD ,0 AS MPE_FVA ,SUM(coalesce(a.USD_PV10_BENCH, 0)) AS MPE_FVA_BPV10 ,0 AS MNE_FVA ,0 AS MNE_FVA_BPV10 ,0 AS FVA_EXPOSURE ,0 AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and    (a.CCC_BUSINESS_AREA IN ( 'CPM' ,'CPM TRADING (MPE)' ,'CREDIT' ,'MS CVA MNE - FID' ,'MS CVA MNE - COMMOD' ) OR a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING' ) ) AND A.CURVE_NAME IN ('cpcr_mpefund') AND ( a.USD_MARKET_VALUE IS NOT NULL OR a.USD_PV10_BENCH IS NOT NULL ) AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME   UNION ALL  SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,a.POSITION_ULTIMATE_CREDIT_PARTY_DARWIN_NAME AS CTPY_NAME ,0 AS MPE_CVA ,SUM(a.USD_MARKET_VALUE) AS MNE_CVA ,0 AS MPE_BPV10 ,SUM(a.USD_PV10_BENCH) AS MNE_BPV10 ,0 AS CVA_EXPOSURE ,0 AS CVA_JTD ,0 AS Hedge_BPV10 ,0 AS Hedge_EXPOSURE ,0 AS Hedge_JTD ,0 AS MPE_FVA ,0 AS MPE_FVA_BPV10 ,0 AS MNE_FVA ,0 AS MNE_FVA_BPV10 ,0 AS FVA_EXPOSURE ,0 AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and    (a.CCC_BUSINESS_AREA IN ( 'CPM' ,'CPM TRADING (MPE)' ,'CREDIT' ,'MS CVA MNE - FID' ,'MS CVA MNE - COMMOD' ) OR a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING' ) ) AND a.PRODUCT_SUB_TYPE_CODE IN ( 'MNE' ,'MNE_CVA' ) AND ( a.USD_MARKET_VALUE IS NOT NULL OR a.USD_PV10_BENCH IS NOT NULL ) AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,a.POSITION_ULTIMATE_CREDIT_PARTY_DARWIN_NAME   UNION ALL  SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME ,0 AS MPE_CVA ,0 AS MPE_CVA_BPV10 ,0 AS MNE_CVA ,0 AS MNE_BPV10 ,0 AS CVA_EXPOSURE ,0 AS CVA_JTD ,0 AS Hedge_BPV10 ,0 AS Hedge_EXPOSURE ,0 AS Hedge_JTD ,0 AS MPE_FVA ,0 AS MPE_FVA_BPV10 ,SUM(a.USD_MARKET_VALUE) AS MNE_FVA ,SUM(coalesce(a.USD_PV10_BENCH, 0)) AS MNE_FVA_BPV10 ,0 AS FVA_EXPOSURE ,0 AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and    (a.CCC_BUSINESS_AREA IN ( 'CPM' ,'CPM TRADING (MPE)' ,'CREDIT' ,'MS CVA MNE - FID' ,'MS CVA MNE - COMMOD' ) OR a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING' ) ) AND a.PRODUCT_SUB_TYPE_CODE IN ( 'MNE_FVA_NET' ,'MNE_FVA' ) AND ( a.USD_MARKET_VALUE IS NOT NULL OR a.USD_PV10_BENCH IS NOT NULL ) AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,CTPY_NAME   UNION ALL  SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,a.CTPY_NAME AS CTPY_NAME ,0 AS MPE_CVA ,0 AS MNE_CVA ,0 AS MPE_BPV10 ,0 AS MNE_BPV10 ,SUM(a.USD_EXPOSURE) AS CVA_EXPOSURE ,SUM(a.USD_DEFAULT_PNL) AS CVA_JTD ,0 AS Hedge_BPV10 ,0 AS Hedge_EXPOSURE ,0 AS Hedge_JTD ,0 AS MPE_FVA ,0 AS MPE_FVA_BPV10 ,0 AS MNE_FVA ,0 AS MNE_FVA_BPV10 ,0 AS FVA_EXPOSURE ,0 AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and    (a.CCC_BUSINESS_AREA IN ( 'CPM' ,'CPM TRADING (MPE)' ,'CREDIT' ,'MS CVA MNE - FID' ,'MS CVA MNE - COMMOD' ) OR a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING' ) ) AND a.PRODUCT_SUB_TYPE_CODE IN ( 'MPE' ,'MPE_CVA' ,'MPE_PROXY' ) AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,a.CTPY_NAME   UNION ALL  SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,a.CTPY_NAME AS CTPY_NAME ,0 AS MPE_CVA ,0 AS MNE_CVA ,0 AS MPE_BPV10 ,0 AS MNE_BPV10 ,0 AS CVA_EXPOSURE ,0 AS CVA_JTD ,0 AS Hedge_BPV10 ,0 AS Hedge_EXPOSURE ,0 AS Hedge_JTD ,0 AS MPE_FVA ,0 AS MPE_FVA_BPV10 ,0 AS MNE_FVA ,0 AS MNE_FVA_BPV10 ,SUM(a.USD_EXPOSURE) AS FVA_EXPOSURE ,SUM(a.USD_DEFAULT_PNL) AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and    (a.CCC_BUSINESS_AREA IN ( 'CPM' ,'CPM TRADING (MPE)' ,'CREDIT' ,'MS CVA MNE - FID' ,'MS CVA MNE - COMMOD' ) OR a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING' ) ) AND a.PRODUCT_SUB_TYPE_CODE IN ( 'MPE_FVA_PROXY' ,'MPE_FVA' ,'MPE_FVA_RAW' ) AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,a.CTPY_NAME   UNION ALL  SELECT a.COB_DATE ,a.CCC_BUSINESS_AREA ,a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME AS CTPY_NAME ,0 AS MPE_CVA ,0 AS MNE_CVA ,0 AS MPE_BPV10 ,0 AS MNE_BPV10 ,0 AS CVA_EXPOSURE ,0 AS CVA_JTD ,SUM(a.USD_PV10_BENCH) AS Hedge_BPV10 ,SUM(a.USD_EXPOSURE) AS Hedge_EXPOSURE ,SUM(a.USD_DEFAULT_PNL) AS Hedge_JTD ,0 AS MPE_FVA ,0 AS MPE_FVA_BPV10 ,0 AS MNE_FVA ,0 AS MNE_FVA_BPV10 ,0 AS FVA_EXPOSURE ,0 AS FVA_JTD FROM cdwuser.U_DM_CVA a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-31') and   a.CCC_STRATEGY IN ( 'MS CVA MPE - DERIVATIVES' ,'CORE MPE CVA' ,'MONOLINE MPE CVA' ,'CPM CREDIT', 'XVA CREDIT' ,'CVA RISK MANAGEMENT' ,'FVA RISK MANAGEMENT' ) AND NOT a.PRODUCT_SUB_TYPE_CODE IN ( 'MPE' ,'MPE_CVA' ,'MPE_PROXY' ,'MNE_CP' ) AND a.PRODUCT_TYPE_CODE IN ( 'DEFSWAP' ,'MUNICDS' ,'LOANCDS' ) AND a.ccc_product_line NOT IN ( 'CREDIT LOAN PORTFOLIO' ,'CMD STRUCTURED FINANCE' ) AND CCC_HIERARCHY_LEVEL9 <> 'INSURANCE PRODUCT' GROUP BY a.COB_DATE ,a.CCC_BUSINESS_AREA ,a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ) as C WHERE CTPY_NAME <> 'MORGAN STANLEY' and CTPY_NAME <> 'UNDEFINED'