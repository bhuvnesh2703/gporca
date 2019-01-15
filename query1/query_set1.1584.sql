WITH IED AS ( SELECT COB_DATE, PROCESS_ID ,POSITION_ID ,sum(D50RAW) AS D50RAW ,Sum(D30RAW) AS D30RAW ,sum(D20RAW) AS D20RAW ,sum(D10RAW) AS D10RAW ,sum(D5RAW) AS D5RAW ,sum(P5RAW) AS P5RAW ,sum(P10RAW) AS P10RAW ,sum(P20RAW) AS P20RAW FROM ( SELECT e.COB_DATE, PROCESS_ID ,POSITION_ID ,sum(e.SLIDE_EQ_MIN_50_USD) AS D50RAW ,sum(e.SLIDE_EQ_MIN_30_USD) AS D30RAW ,sum(e.SLIDE_EQ_MIN_20_USD) AS D20RAW ,sum(e.SLIDE_EQ_MIN_10_USD) AS D10RAW ,sum(e.SLIDE_EQ_MIN_05_USD) AS D5RAW ,sum(e.SLIDE_EQ_PLS_05_USD) AS P5RAW ,sum(e.SLIDE_EQ_PLS_10_USD) AS P10RAW ,sum(e.SLIDE_EQ_PLS_20_USD) AS P20RAW FROM CDWUSER.U_EQ_MSR e WHERE e.COB_DATE in ('2018-02-28','2018-02-27') AND e.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND e.CCC_PL_REPORTING_REGION = 'EMEA' AND SILO_SRC = 'IED' AND e.CCC_DIVISION <> 'FID DVA' AND e.CCC_DIVISION <> 'FIC DVA' AND CCC_STRATEGY <> 'MS DVA STR NOTES IED' GROUP BY e.PROCESS_ID ,POSITION_ID, e.COB_DATE ) x GROUP BY PROCESS_ID ,POSITION_ID, COB_DATE ) ,CountryWeights AS ( SELECT d.COB_DATE ,PROCESS_ID ,POSITION_ID, CASE WHEN d.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN d.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN d.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END AS CCC_PL_REPORTING_REGION, CASE WHEN d.CCC_TAPS_COMPANY = '0302' THEN 'MSIP' WHEN d.CCC_TAPS_COMPANY = '0517' THEN 'MSBIL' WHEN d.CCC_TAPS_COMPANY = '0319' THEN 'MSIM' ELSE 'OTHER' END as CCC_TAPS_COMPANY, CASE WHEN (d.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR d.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (d.CCC_DIVISION = 'FID DVA' OR d.CCC_DIVISION='FIC DVA' OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN d.CCC_DIVISION = 'BANK RESOURCE MANAGEMENT' THEN 'BRM' WHEN d.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' THEN 'TCM' WHEN d.CCC_DIVISION ='NON CORE' THEN 'NON CORE' WHEN (d.CCC_DIVISION = 'FIXED INCOME DIVISION' AND d.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','EM CREDIT TRADING','FXEM MACRO TRADING','STRUCTURED RATES','LIQUID FLOW RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHER FID' WHEN (d.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND d.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'OTHER IED' ELSE d.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, CASE WHEN d.ISSUER_COUNTRY_CODE_DECOMP IN ('USA','CAN') THEN 'NAM' WHEN d.ISSUER_COUNTRY_CODE_DECOMP IN ('ALB'	,'AND','AUT','BLR'	,'BEL'	,'BIH'	,'BGR','CYP'	,'HRV','CZE'	,'DNK','EST'	,'FRO','FIN','FRA','GEO','DEU','GRC','GGY','VAT','HUN','ISL','IRL','IMN','ITA','JEY','LVA','LIE','LTU','LUX','MKD','MLT','MDA','MCO','MNE','NLD','NOR','POL','PRT','ROU','RUS','SMR','SRB','SVK','SVN','ESP','SJM','SWE','CHE','UKR') THEN 'Europe' WHEN d.ISSUER_COUNTRY_CODE_DECOMP IN ('GBR') THEN 'UK' WHEN d.ISSUER_COUNTRY_CODE_DECOMP IN ('CHN') THEN 'China' WHEN d.ISSUER_COUNTRY_CODE_DECOMP IN ('JPN') THEN 'Japan' ELSE 'Other' END as AREA ,ISSUER_COUNTRY_CODE_DECOMP AS COUNTRY ,abs(sum(PRODUCT_WEIGHT_DECOMP)) AS WEIGHT FROM CDWUSER.U_DECOMP_MSR d WHERE d.COB_DATE in ('2018-02-28','2018-02-27') AND d.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND d.CCC_PL_REPORTING_REGION = 'EMEA' AND d.CCC_DIVISION <> 'FID DVA' AND d.CCC_DIVISION <> 'FIC DVA' AND CCC_STRATEGY <> 'MS DVA STR NOTES IED' AND SILO_SRC = 'IED' GROUP BY d.COB_DATE ,PROCESS_ID ,POSITION_ID ,ISSUER_COUNTRY_CODE_DECOMP ,CCC_DIVISION ,CCC_BUSINESS_AREA ,CCC_STRATEGY ,CCC_PL_REPORTING_REGION ,CCC_TAPS_COMPANY HAVING sum(PRODUCT_WEIGHT_DECOMP) <> 0 ) ,GrossWeights AS ( SELECT x.COB_DATE ,PROCESS_ID ,POSITION_ID ,sum(abs(WEIGHT)) AS GROSS_WEIGHT FROM CountryWeights x GROUP BY x.COB_DATE ,PROCESS_ID ,POSITION_ID ) ,Decomp AS ( SELECT w.COB_DATE ,w.Process_ID ,w.Position_id ,COUNTRY ,CCC_PL_REPORTING_REGION ,CCC_TAPS_COMPANY ,CCC_BUSINESS_AREA ,AREA ,abs(WEIGHT / GROSS_WEIGHT) AS WEIGHT FROM CountryWeights w INNER JOIN GrossWeights g ON ( w.cob_date = g.cob_date AND w.process_id = g.process_id AND w.position_id = g.position_id ) ) SELECT VIEW ,COB_DATE ,CCC_PL_REPORTING_REGION ,CCC_TAPS_COMPANY ,CCC_BUSINESS_AREA ,AREA ,COUNTRY ,sum(D50RAW) AS D50RAW ,sum(D30RAW+(17*(D50RAW-D30RAW)/20)) AS D47RAW ,sum(D30RAW+(2*(D50RAW-D30RAW)/20)) AS D32RAW ,sum(D30RAW) AS D30RAW ,sum(D30RAW + D20RAW) / 2 AS D25RAW ,sum((D30RAW + D20RAW) * 3 / 10 + D20RAW * 2 / 5) AS D23RAW ,sum(D20RAW+(2*(D30RAW-D20RAW)/10)) AS D22RAW ,sum(D20RAW) AS D20RAW ,sum(D10RAW + (6*(D20RAW-D10RAW)/10)) AS D16RAW ,sum(D20RAW + D10RAW) / 2 AS D15RAW ,sum((D20RAW + D10RAW) * 3 / 10 + D10RAW * 2 / 5) AS D13RAW ,sum(D10RAW+(2*(D20RAW-D10RAW)/10)) AS D12RAW ,sum(D10RAW) AS D10RAW ,sum(D5RAW) AS D5RAW ,sum(P5RAW) AS P5RAW ,sum(P10RAW) AS P10RAW ,sum(P10RAW + P20RAW) / 2 AS P15RAW ,sum(P20RAW) AS P20RAW FROM ( SELECT 'Decomp' AS VIEW, i.COB_DATE, CCC_PL_REPORTING_REGION, CCC_TAPS_COMPANY, CCC_BUSINESS_AREA, AREA ,COUNTRY ,(D50RAW * WEIGHT) AS D50RAW ,(D30RAW * WEIGHT) AS D30RAW ,(D20RAW * WEIGHT) AS D20RAW ,(D10RAW * WEIGHT) AS D10RAW ,(D5RAW * WEIGHT) AS D5RAW ,(P5RAW * WEIGHT) AS P5RAW ,(P10RAW * WEIGHT) AS P10RAW ,(P20RAW * WEIGHT) AS P20RAW FROM IED i INNER JOIN Decomp d ON ( i.PROCESS_ID = d.PROCESS_ID AND i.POSITION_ID = d.POSITION_ID and i.COB_DATE = d.COB_DATE ) ) x GROUP BY VIEW ,Country ,COB_DATE ,CCC_PL_REPORTING_REGION ,CCC_TAPS_COMPANY ,CCC_BUSINESS_AREA ,AREA