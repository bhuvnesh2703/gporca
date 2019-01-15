WITH EQ_POPULATION AS ( SELECT v.COB_DATE ,d.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ,v.CCC_PL_REPORTING_REGION ,v.FID1_SENIORITY ,v.CCC_DIVISION ,v.CCC_BUSINESS_AREA ,v.CCC_PRODUCT_LINE ,v.CCC_STRATEGY ,v.SPG_DESC ,v.PRODUCT_TYPE_CODE ,v.VERTICAL_SYSTEM ,v.TERM_BUCKET ,v.FID1_INDUSTRY_NAME_LEVEL1 ,v.FID1_INDUSTRY_NAME_LEVEL2 ,v.ISSUER_COUNTRY_CODE ,v.UNDERLIER_TICK ,v.UNDERLIER_EXCH ,v.MRD_RATING ,v.LE_GROUP ,v.CCC_TAPS_COMPANY ,v.PARENT_LEGAL_ENTITY ,SUM(d.PRODUCT_WEIGHT_DECOMP * v.USD_PV01SPRD)::double precision AS USD_PV01SPRD ,SUM(d.PRODUCT_WEIGHT_DECOMP * v.USD_EXPOSURE)::double precision AS USD_EXPOSURE ,SUM(d.PRODUCT_WEIGHT_DECOMP * v.USD_PV10_BENCH)::double precision AS USD_PV10_BENCH ,SUM(d.PRODUCT_WEIGHT_DECOMP * v.USD_CREDIT_PV10PCT)::double precision AS USD_CREDIT_PV10PCT FROM ( SELECT v.POSITION_KEY ,v.PROCESS_ID ,v.COB_DATE ,v.CCC_PL_REPORTING_REGION ,v.FID1_SENIORITY ,v.CCC_DIVISION ,v.CCC_BUSINESS_AREA ,v.CCC_PRODUCT_LINE ,v.CCC_STRATEGY ,v.SPG_DESC ,v.PRODUCT_TYPE_CODE ,v.VERTICAL_SYSTEM ,v.TERM_BUCKET ,v.FID1_INDUSTRY_NAME_LEVEL1 ,v.FID1_INDUSTRY_NAME_LEVEL2 ,v.ISSUER_COUNTRY_CODE ,v.UNDERLIER_TICK ,v.UNDERLIER_EXCH ,v.MRD_RATING ,v.LE_GROUP ,v.CCC_TAPS_COMPANY ,v.PARENT_LEGAL_ENTITY ,SUM(v.USD_PV01SPRD) AS USD_PV01SPRD ,SUM(v.USD_EXPOSURE) AS USD_EXPOSURE ,SUM(v.USD_PV10_BENCH) AS USD_PV10_BENCH ,SUM(v.USD_CREDIT_PV10PCT) AS USD_CREDIT_PV10PCT FROM CDWUSER.U_EXP_MSR v WHERE v.COB_DATE IN ('2018-02-28','2018-02-27') AND (v.LE_GROUP = 'UK' OR v.CCC_PL_REPORTING_REGION = 'EMEA') AND v.VERTICAL_SYSTEM LIKE '%EQ%' AND (USD_PV01SPRD <> 0 OR USD_EXPOSURE <> 0 OR USD_PV10_BENCH <> 0 OR USD_CREDIT_PV10PCT <> 0) GROUP BY v.POSITION_KEY ,v.PROCESS_ID ,v.COB_DATE ,v.CCC_PL_REPORTING_REGION ,v.FID1_SENIORITY ,v.CCC_DIVISION ,v.CCC_BUSINESS_AREA ,v.CCC_PRODUCT_LINE ,v.CCC_STRATEGY ,v.SPG_DESC ,v.PRODUCT_TYPE_CODE ,v.VERTICAL_SYSTEM ,v.TERM_BUCKET ,v.FID1_INDUSTRY_NAME_LEVEL1 ,v.FID1_INDUSTRY_NAME_LEVEL2 ,v.ISSUER_COUNTRY_CODE ,v.UNDERLIER_TICK ,v.UNDERLIER_EXCH ,v.MRD_RATING ,v.LE_GROUP ,v.CCC_TAPS_COMPANY ,v.PARENT_LEGAL_ENTITY ) v INNER JOIN ( SELECT d.COB_DATE ,d.POSITION_KEY ,d.PROCESS_ID ,CASE WHEN d.CREDIT_RISK_ISSUER_NAME IN ('UNDEFINED', 'UNKNOWN') THEN d.CHILD_ISSUER_PARTY_DARWIN_NAME ELSE d.CREDIT_RISK_ISSUER_NAME END POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ,SUM(d.PRODUCT_WEIGHT_DECOMP) PRODUCT_WEIGHT_DECOMP FROM CDWUSER.U_DECOMP_MSR d WHERE d.COB_DATE IN ('2018-02-28','2018-02-27') AND d.PRODUCT_WEIGHT_DECOMP <> 0 AND (d.LE_GROUP = 'UK' OR d.CCC_PL_REPORTING_REGION = 'EMEA') AND d.VERTICAL_SYSTEM LIKE '%EQ%' GROUP BY COB_DATE ,d.POSITION_KEY ,d.PROCESS_ID ,CASE WHEN d.CREDIT_RISK_ISSUER_NAME IN ('UNDEFINED', 'UNKNOWN') THEN d.CHILD_ISSUER_PARTY_DARWIN_NAME ELSE d.CREDIT_RISK_ISSUER_NAME END ) d ON v.COB_DATE = d.COB_DATE AND v.POSITION_KEY = d.POSITION_KEY AND v.PROCESS_ID = d.PROCESS_ID WHERE v.COB_DATE IN ('2018-02-28','2018-02-27') AND (v.LE_GROUP = 'UK' OR v.CCC_PL_REPORTING_REGION = 'EMEA') AND v.VERTICAL_SYSTEM LIKE '%EQ%' AND (USD_PV01SPRD <> 0 OR USD_EXPOSURE <> 0 OR USD_PV10_BENCH <> 0 OR USD_CREDIT_PV10PCT <> 0) GROUP BY v.COB_DATE ,d.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ,v.CCC_PL_REPORTING_REGION ,v.FID1_SENIORITY ,v.CCC_DIVISION ,v.CCC_BUSINESS_AREA ,v.CCC_PRODUCT_LINE ,v.CCC_STRATEGY ,v.SPG_DESC ,v.PRODUCT_TYPE_CODE ,v.VERTICAL_SYSTEM ,v.TERM_BUCKET ,v.FID1_INDUSTRY_NAME_LEVEL1 ,v.FID1_INDUSTRY_NAME_LEVEL2 ,v.ISSUER_COUNTRY_CODE ,v.UNDERLIER_TICK ,v.UNDERLIER_EXCH ,v.MRD_RATING ,v.LE_GROUP ,v.CCC_TAPS_COMPANY ,v.PARENT_LEGAL_ENTITY ) , MAIN_POPULATION AS ( SELECT COB_DATE ,POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ,CCC_PL_REPORTING_REGION ,FID1_SENIORITY ,CCC_DIVISION ,CCC_BUSINESS_AREA ,CCC_PRODUCT_LINE ,CCC_STRATEGY ,SPG_DESC ,PRODUCT_TYPE_CODE ,VERTICAL_SYSTEM ,TERM_BUCKET ,FID1_INDUSTRY_NAME_LEVEL1 ,FID1_INDUSTRY_NAME_LEVEL2 ,ISSUER_COUNTRY_CODE ,UNDERLIER_TICK ,UNDERLIER_EXCH ,MRD_RATING ,LE_GROUP ,CCC_TAPS_COMPANY ,PARENT_LEGAL_ENTITY ,sum(USD_PV01SPRD)::double precision as USD_PV01SPRD ,sum(USD_EXPOSURE)::double precision as USD_EXPOSURE ,sum(USD_PV10_BENCH)::double precision as USD_PV10_BENCH ,sum(USD_CREDIT_PV10PCT)::double precision as USD_CREDIT_PV10PCT FROM CDWUSER.U_EXP_MSR A WHERE COB_DATE IN ('2018-02-28','2018-02-27') AND (LE_GROUP ='UK' OR CCC_PL_REPORTING_REGION ='EMEA') AND a.VERTICAL_SYSTEM not LIKE '%EQ%' GROUP BY COB_DATE ,POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ,CCC_PL_REPORTING_REGION ,FID1_SENIORITY ,CCC_DIVISION ,CCC_BUSINESS_AREA ,CCC_PRODUCT_LINE ,CCC_STRATEGY ,SPG_DESC ,PRODUCT_TYPE_CODE ,VERTICAL_SYSTEM ,TERM_BUCKET ,FID1_INDUSTRY_NAME_LEVEL1 ,FID1_INDUSTRY_NAME_LEVEL2 ,ISSUER_COUNTRY_CODE ,UNDERLIER_TICK ,UNDERLIER_EXCH ,MRD_RATING ,LE_GROUP ,CCC_TAPS_COMPANY ,PARENT_LEGAL_ENTITY ) SELECT VERTICAL_SYSTEM, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PL_REPORTING_REGION, CASE WHEN FID1_SENIORITY IN ('AT1', 'SUBT1', 'SUBUT2') THEN 'Junior Subordinate' WHEN CCC_PRODUCT_LINE IN ('DISTRESSED TRADING') THEN 'Distressed Trading' WHEN (CCC_BUSINESS_AREA IN ('CREDIT-SECURITIZED PRODS', 'SECURITIZED PRODUCTS GRP', 'COMMERCIAL RE (PTG)', 'RESIDENTIAL') AND SPG_DESC NOT IN ('CORPORATE BONDS', 'CORPORATE DEFAULT SWAP', 'SWAP', 'GOVERNMENT')) THEN 'SPG' ELSE 'spreadsensexpo' END AS POPULATION, CASE WHEN CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS','CREL BANK HFI','WAREHOUSE') THEN 'WHAREHOUSE_CRELENDING' WHEN SPG_DESC IN ('CMBS DEFAULT SWAP', 'CMBS INDEX', 'CMBS IO', 'CMBS LOAN', 'CMBS SECURITY') THEN 'CMBS' WHEN SPG_DESC IN ('CORPORATE CDO', 'CORPORATE CDO DEFAULT SWAP', 'CORPORATE CDO PREFERRED', 'CORPORATE CLO', 'CORPORATE CLO TRUPS') THEN 'CLO' WHEN SPG_DESC IN ('ABS AUTO LOAN & SECURITY', 'ABS OTHER SECURITY') THEN 'ABS' WHEN (SPG_DESC LIKE ('%RMBS PRIME%')) THEN 'RMBS PRIME' WHEN SPG_DESC IN ('RMBS SUB PRIME SECURITY','RMBS SUB PRIME SECOND PAY','RMBS SUB PRIME RESIDUAL','RMBS SUB PRIME INDEX','RMBS NON CONFORMING DEFAULT SWAP','RMBS MBX INDEX','RMBS IOS INDEX','RMBS DEFAULT SWAP','RMBS CDO EQUITY','RMBS CDO','RMBS ALTA SECURITY') THEN 'RMBS NON CONFORMING' ELSE 'NN SPG' END AS EU_CATEGORY, CASE WHEN PRODUCT_TYPE_CODE IN ('CRDBSKT','CRDINDEX','CDSOPTIDX') THEN 'index products' WHEN TERM_BUCKET IN ('0-0.083Y','0.083-0.25Y','0.25-0.5Y','0.5-0.75Y','0.75-1Y','1-2Y','2-3Y','3-5Y') THEN '<5Y' WHEN TERM_BUCKET IN ('5-7Y', '7-8Y', '8-10Y', '10-12Y', '12-15Y') THEN '5-15Y' WHEN TERM_BUCKET IN ('15-20Y','20-25Y','25-30Y','30-40Y','40-50Y','50-60Y','60-75Y','75+Y') THEN '>15Y' ELSE 'Other' END AS MATURITY_BUCKET, CASE WHEN (VERTICAL_SYSTEM LIKE'%EQUITY%' AND PRODUCT_TYPE_CODE IN ('FUTURE')) THEN 'excluded' ELSE 'included' END AS EXCLFLAG, CASE WHEN (FID1_INDUSTRY_NAME_LEVEL1 IN ('SOVEREIGN', 'GOVERNMENT SPONSORED') OR (FID1_INDUSTRY_NAME_LEVEL1 = 'N/A' AND ISSUER_COUNTRY_CODE = 'XS')) THEN 'SOVEREIGN' WHEN UNDERLIER_TICK ||'.'||underlier_Exch IN ('tlt.p') THEN 'SOVEREIGN' WHEN (VERTICAL_SYSTEM LIKE'%EQUITY%' AND PRODUCT_TYPE_CODE IN ('FUTURE')) THEN 'SOVEREIGN' ELSE 'NNSOVEREIGN' END AS SOVNN, CASE WHEN ISSUER_COUNTRY_CODE IN ('XS', 'VGB', 'USA', 'SWE', 'PRT', 'NZL', 'NOR', 'NLD', 'LUX', 'JPN', 'JEY', 'ITA', 'ISL', 'IRL', 'IRL', 'IMN', 'GRC', 'GGY', 'GBR', 'FRA', 'FIN', 'ESP', 'DNK', 'DEU', 'CYP', 'CYM', 'CHE', 'CAN', 'BMU', 'BEL', 'AUT', 'AUS') THEN 'G10' ELSE 'EM' END AS EMFLAG, CASE WHEN MRD_RATING IN ('AAA', 'AA', 'A', 'BBB') THEN 'IG' ELSE 'NIG' END AS RATING2, CASE WHEN FID1_INDUSTRY_NAME_LEVEL2 LIKE '%FINANCIAL%' THEN 'FINANCIALS' WHEN FID1_INDUSTRY_NAME_LEVEL2 = 'ENERGY' THEN 'ENERGY' ELSE 'Others' END AS INDUSTRY, ISSUER_COUNTRY_CODE, PRODUCT_TYPE_CODE, CASE WHEN CCC_DIVISION = 'COMMODITIES' THEN 'COMMODITIES' WHEN (CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION' AND CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'OTHERS' WHEN (CCC_DIVISION='FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','LENDING','FXEM MACRO TRADING','EM CREDIT TRADING','LIQUID FLOW RATES','STRUCTURED RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHERS FID' ELSE CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, CASE WHEN (CCC_BUSINESS_AREA IN ( 'CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (CCC_DIVISION IN ('FID DVA', 'FIC DVA') OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN CCC_DIVISION = 'COMMODITIES' THEN 'FIXED INCOME DIVISION' ELSE CCC_DIVISION END AS CCC_DIVISION, CASE WHEN LE_GROUP ='UK' THEN 'Y' ELSE 'N' END AS IS_UK_GROUP, CASE WHEN CCC_TAPS_COMPANY IN ('7787','7786','7723','7728','7773','7772','7721','4884','0319') THEN 'MSIM' WHEN PARENT_LEGAL_ENTITY IN ('0517(G)') THEN 'MSBIL' WHEN PARENT_LEGAL_ENTITY= '0302(G)' THEN 'MSIP' WHEN LE_GROUP = 'UK' THEN 'OtherUKG' ELSE 'NOTUKG' END AS entityclassification, CCC_PRODUCT_LINE, SUM (Case when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then coalesce(USD_PV10_BENCH,USD_CREDIT_PV10PCT) else 0 end ) AS USD_PV10_BENCHUK, SUM (Case when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then coalesce(USD_PV10_BENCH,USD_CREDIT_PV10PCT) when (LE_GROUP = 'UK' AND cob_date = '2018-02-27') then -coalesce(USD_PV10_BENCH,USD_CREDIT_PV10PCT) else 0 end ) AS USD_PV10_BENCHUKchng, SUM (Case when (CCC_PL_REPORTING_REGION = 'EMEA' AND cob_date = '2018-02-28') then coalesce(USD_PV10_BENCH,USD_CREDIT_PV10PCT) else 0 end ) AS USD_PV10_BENCHEMEA, SUM (CASE WHEN (CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 0 when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then coalesce(USD_PV10_BENCH,USD_CREDIT_PV10PCT) else 0 end ) AS USD_PV10_BENCHUKxCVA, SUM (Case when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then USD_EXPOSURE else 0 end ) AS USD_EXPOSUREUK, SUM (Case when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then USD_EXPOSURE when (LE_GROUP = 'UK' AND cob_date = '2018-02-27') then -USD_EXPOSURE else 0 end ) AS USD_EXPOSUREUKchng, SUM (Case when (CCC_PL_REPORTING_REGION = 'EMEA' AND cob_date = '2018-02-28') then USD_EXPOSURE else 0 end ) AS USD_EXPOSUREEMEA, SUM (CASE WHEN (CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 0 when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then USD_EXPOSURE else 0 end )AS USD_EXPOSUREUKxCVA, SUM (Case when (LE_GROUP = 'UK' AND cob_date = '2018-02-28' AND vertical_system like'%EQUITY%') then USD_PV01SPRD/1000 when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then USD_PV01SPRD else 0 end ) AS USD_PV01SPRDUK, SUM (Case when (LE_GROUP = 'UK' AND cob_date = '2018-02-28' AND vertical_system like'%EQUITY%') then USD_PV01SPRD/1000 when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then USD_PV01SPRD when (LE_GROUP = 'UK' AND cob_date = '2018-02-27' AND vertical_system like'%EQUITY%') then -USD_PV01SPRD/1000 when (LE_GROUP = 'UK' AND cob_date = '2018-02-27') then -USD_PV01SPRD else 0 end ) AS USD_PV01SPRDUKchng, SUM (Case when (CCC_PL_REPORTING_REGION = 'EMEA' AND cob_date = '2018-02-28' AND vertical_system like'%EQUITY%') then USD_PV01SPRD/1000 when (CCC_PL_REPORTING_REGION = 'EMEA' AND cob_date = '2018-02-28') then USD_PV01SPRD else 0 end ) AS USD_PV01SPRDEMEA, SUM (CASE WHEN (CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 0 when (LE_GROUP = 'UK' AND cob_date = '2018-02-28' AND vertical_system like'%EQUITY%') then USD_PV01SPRD/1000 when (LE_GROUP = 'UK' AND cob_date = '2018-02-28') then USD_PV01SPRD else 0 end) AS USD_PV01SPRDUKxCVA FROM ( SELECT * FROM MAIN_POPULATION UNION ALL SELECT * FROM EQ_POPULATION ) y GROUP BY VERTICAL_SYSTEM, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PL_REPORTING_REGION, CASE WHEN FID1_SENIORITY IN ('AT1', 'SUBT1', 'SUBUT2') THEN 'Junior Subordinate' WHEN CCC_PRODUCT_LINE IN ('DISTRESSED TRADING') THEN 'Distressed Trading' WHEN (CCC_BUSINESS_AREA IN ('CREDIT-SECURITIZED PRODS', 'SECURITIZED PRODUCTS GRP', 'COMMERCIAL RE (PTG)', 'RESIDENTIAL') AND SPG_DESC NOT IN ('CORPORATE BONDS', 'CORPORATE DEFAULT SWAP', 'SWAP', 'GOVERNMENT')) THEN 'SPG' ELSE 'spreadsensexpo' END, CASE WHEN CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS','CREL BANK HFI','WAREHOUSE') THEN 'WHAREHOUSE_CRELENDING' WHEN SPG_DESC IN ('CMBS DEFAULT SWAP', 'CMBS INDEX', 'CMBS IO', 'CMBS LOAN', 'CMBS SECURITY') THEN 'CMBS' WHEN SPG_DESC IN ('CORPORATE CDO', 'CORPORATE CDO DEFAULT SWAP', 'CORPORATE CDO PREFERRED', 'CORPORATE CLO', 'CORPORATE CLO TRUPS') THEN 'CLO' WHEN SPG_DESC IN ('ABS AUTO LOAN & SECURITY', 'ABS OTHER SECURITY') THEN 'ABS' WHEN (SPG_DESC LIKE ('%RMBS PRIME%')) THEN 'RMBS PRIME' WHEN SPG_DESC IN ('RMBS SUB PRIME SECURITY','RMBS SUB PRIME SECOND PAY','RMBS SUB PRIME RESIDUAL','RMBS SUB PRIME INDEX','RMBS NON CONFORMING DEFAULT SWAP','RMBS MBX INDEX','RMBS IOS INDEX','RMBS DEFAULT SWAP','RMBS CDO EQUITY','RMBS CDO','RMBS ALTA SECURITY') THEN 'RMBS NON CONFORMING' ELSE 'NN SPG' END, CASE WHEN PRODUCT_TYPE_CODE IN ('CRDBSKT','CRDINDEX','CDSOPTIDX') THEN 'index products' WHEN TERM_BUCKET IN ('0-0.083Y','0.083-0.25Y','0.25-0.5Y','0.5-0.75Y','0.75-1Y','1-2Y','2-3Y','3-5Y') THEN '<5Y' WHEN TERM_BUCKET IN ('5-7Y', '7-8Y', '8-10Y', '10-12Y', '12-15Y') THEN '5-15Y' WHEN TERM_BUCKET IN ('15-20Y','20-25Y','25-30Y','30-40Y','40-50Y','50-60Y','60-75Y','75+Y') THEN '>15Y' ELSE 'Other' END, CASE WHEN (VERTICAL_SYSTEM LIKE'%EQUITY%' AND PRODUCT_TYPE_CODE IN ('FUTURE')) THEN 'excluded' ELSE 'included' END, CASE WHEN (FID1_INDUSTRY_NAME_LEVEL1 IN ('SOVEREIGN', 'GOVERNMENT SPONSORED') OR (FID1_INDUSTRY_NAME_LEVEL1 = 'N/A' AND ISSUER_COUNTRY_CODE = 'XS')) THEN 'SOVEREIGN' WHEN UNDERLIER_TICK ||'.'||underlier_Exch IN ('tlt.p') THEN 'SOVEREIGN' WHEN (VERTICAL_SYSTEM LIKE'%EQUITY%' AND PRODUCT_TYPE_CODE IN ('FUTURE')) THEN 'SOVEREIGN' ELSE 'NNSOVEREIGN' END, CASE WHEN ISSUER_COUNTRY_CODE IN ('XS', 'VGB', 'USA', 'SWE', 'PRT', 'NZL', 'NOR', 'NLD', 'LUX', 'JPN', 'JEY', 'ITA', 'ISL', 'IRL', 'IRL', 'IMN', 'GRC', 'GGY', 'GBR', 'FRA', 'FIN', 'ESP', 'DNK', 'DEU', 'CYP', 'CYM', 'CHE', 'CAN', 'BMU', 'BEL', 'AUT', 'AUS') THEN 'G10' ELSE 'EM' END, CASE WHEN MRD_RATING IN ('AAA', 'AA', 'A', 'BBB') THEN 'IG' ELSE 'NIG' END, CASE WHEN MRD_RATING IN ('AAA', 'AA', 'A', 'BBB') THEN 'IG' ELSE 'NIG' END, CASE WHEN FID1_INDUSTRY_NAME_LEVEL2 LIKE '%FINANCIAL%' THEN 'FINANCIALS' WHEN FID1_INDUSTRY_NAME_LEVEL2 = 'ENERGY' THEN 'ENERGY' ELSE 'Others' END, ISSUER_COUNTRY_CODE, PRODUCT_TYPE_CODE, CASE WHEN CCC_DIVISION = 'COMMODITIES' THEN 'COMMODITIES' WHEN (CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION' AND CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'OTHERS' WHEN (CCC_DIVISION='FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','LENDING','FXEM MACRO TRADING','EM CREDIT TRADING','LIQUID FLOW RATES','STRUCTURED RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHERS FID' ELSE CCC_BUSINESS_AREA END, CASE WHEN (CCC_BUSINESS_AREA IN ( 'CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (CCC_DIVISION IN ('FID DVA', 'FIC DVA') OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN CCC_DIVISION = 'COMMODITIES' THEN 'FIXED INCOME DIVISION' ELSE CCC_DIVISION END, CASE WHEN LE_GROUP ='UK' THEN 'Y' ELSE 'N' END, CASE WHEN CCC_TAPS_COMPANY IN ('7787','7786','7723','7728','7773','7772','7721','4884','0319') THEN 'MSIM' WHEN PARENT_LEGAL_ENTITY IN ('0517(G)') THEN 'MSBIL' WHEN PARENT_LEGAL_ENTITY= '0302(G)' THEN 'MSIP' WHEN LE_GROUP = 'UK' THEN 'OtherUKG' ELSE 'NOTUKG' END, CCC_PRODUCT_LINE