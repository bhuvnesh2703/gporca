SELECT a.COB_DATE, CASE WHEN (a.ISSUER_COUNTRY_CODE IN ('VEN') OR (a.CCC_STRATEGY IN ('DISTRESSED TRADING1') AND a.COUNTRY_GROUP NOT IN ('Other'))) THEN 'Distressed' WHEN a.ISSUER_COUNTRY_CODE IN ('AGO','CMR','ETH','GEO','MNE','RWA','TZA') THEN 'EMEA' WHEN a.ISSUER_COUNTRY_CODE IN ('ARM') THEN 'CIS' WHEN a.ISSUER_COUNTRY_CODE IN ('BGD') THEN 'ASIA' ELSE a.COUNTRY_GROUP END AS COUNTRY_GROUP, SUM(a.USD_EXPOSURE) AS Net_Exposure, CASE WHEN (a.CCC_BUSINESS_AREA IN ( 'CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION IN ('FID DVA', 'FIC DVA') OR a.CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'FIXED INCOME DIVISION' ELSE a.CCC_DIVISION END AS CCC_DIVISION, a.IS_UK_GROUP, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END AS CCC_PL_REPORTING_REGION, CASE WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'COMMODITIES' WHEN (a.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'OTHERS' WHEN (a.CCC_DIVISION='FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','FXEM MACRO TRADING','EM CREDIT TRADING','LIQUID FLOW RATES','STRUCTURED RATES','SECURITIZED PRODUCTS GRP','COMMODITIES')) THEN 'OTHERS FID' ELSE a.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, CASE WHEN a.CCC_TAPS_COMPANY='0319' THEN 'MSIM' WHEN a.CCC_TAPS_COMPANY IN ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY='0302' THEN 'MSIP' WHEN a.IS_UK_GROUP = 'Y' THEN 'OtherUKG' ELSE 'NOTUKG' END AS entityclassification FROM cdwuser.U_DM_FX a WHERE a.cob_date IN ('2018-02-28','2018-02-21') AND a.IS_MORGAN_STANLEY='N' AND a.ccc_business_area NOT IN ('LENDING') AND a.USD_EXPOSURE IS NOT NULL GROUP BY a.COB_DATE, CASE WHEN (a.ISSUER_COUNTRY_CODE IN ('VEN') OR (a.CCC_STRATEGY IN ('DISTRESSED TRADING1') AND a.COUNTRY_GROUP NOT IN ('Other'))) THEN 'Distressed' WHEN a.ISSUER_COUNTRY_CODE IN ('AGO','CMR','ETH','GEO','MNE','RWA','TZA') THEN 'EMEA' WHEN a.ISSUER_COUNTRY_CODE IN ('ARM') THEN 'CIS' WHEN a.ISSUER_COUNTRY_CODE IN ('BGD') THEN 'ASIA' ELSE a.COUNTRY_GROUP END, CASE WHEN (a.CCC_BUSINESS_AREA IN ( 'CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION IN ('FID DVA', 'FIC DVA') OR a.CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'FIXED INCOME DIVISION' ELSE a.CCC_DIVISION END, a.IS_UK_GROUP, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END, CASE WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'COMMODITIES' WHEN (a.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'OTHERS' WHEN (a.CCC_DIVISION='FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','FXEM MACRO TRADING','EM CREDIT TRADING','LIQUID FLOW RATES','STRUCTURED RATES','SECURITIZED PRODUCTS GRP','COMMODITIES')) THEN 'OTHERS FID' ELSE a.CCC_BUSINESS_AREA END, CASE WHEN a.CCC_TAPS_COMPANY='0319' THEN 'MSIM' WHEN a.CCC_TAPS_COMPANY IN ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY='0302' THEN 'MSIP' WHEN a.IS_UK_GROUP = 'Y' THEN 'OtherUKG' ELSE 'NOTUKG' END