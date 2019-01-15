With Top10UKName as ( SELECT CASE WHEN a.PRODUCT_DESCRIPTION_DECOMP IS NULL THEN 'UNDEFINED' ELSE a.PRODUCT_DESCRIPTION_DECOMP END PRODUCT_DESCRIPTION_DECOMP, sum(a.USD_EQ_DELTA_DECOMP) as USD_EQ_DELTA_DECOMP FROM CDWUSER.U_DECOMP_MSR a WHERE a.COB_DATE = '2018-02-28' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BANKING_TRADING ='TRADING' AND a.LE_GROUP = 'UK' AND ABS(a.USD_EQ_DELTA_DECOMP) > 0 GROUP BY a.PRODUCT_DESCRIPTION_DECOMP ORDER BY ABS(sum(a.USD_EQ_DELTA_DECOMP)) DESC FETCH FIRST 10 ROWS ONLY ) SELECT 'UK GROUP' as CUT, a.COB_DATE, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END AS CCC_PL_REPORTING_REGION, CASE WHEN (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION = 'FID DVA' OR a.CCC_DIVISION='FIC DVA' OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'BANK RESOURCE MANAGEMENT' THEN 'BRM' WHEN a.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' THEN 'TCM' WHEN a.CCC_DIVISION ='NON CORE' THEN 'NON CORE' WHEN (a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','EM CREDIT TRADING','FXEM MACRO TRADING','STRUCTURED RATES','LIQUID FLOW RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHER FID' WHEN (a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'IED OTHER' ELSE a.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, CASE WHEN a.CCC_TAPS_COMPANY = '0302' THEN 'MSIP' WHEN a.CCC_TAPS_COMPANY in ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY = '0319' THEN 'MSIM' ELSE 'OTHER' END as CCC_TAPS_COMPANY, CASE WHEN a.PRODUCT_DESCRIPTION_DECOMP IS NULL THEN 'UNDEFINED' ELSE a.PRODUCT_DESCRIPTION_DECOMP END PRODUCT_DESCRIPTION_DECOMP, sum(a.USD_EQ_DELTA_DECOMP)/1000 as USD_DELTA FROM CDWUSER.U_DECOMP_MSR a WHERE a.COB_DATE in ('2018-02-28','2018-02-21') AND a.LE_GROUP = 'UK' AND a.PRODUCT_DESCRIPTION_DECOMP IN (SELECT PRODUCT_DESCRIPTION_DECOMP FROM TOP10UKName) AND a.CCC_DIVISION <> 'FID DVA' AND a.CCC_DIVISION <> 'FIC DVA' AND CCC_STRATEGY <> 'MS DVA STR NOTES IED' GROUP BY a.COB_DATE, a.PRODUCT_DESCRIPTION_DECOMP, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END, CASE WHEN (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION = 'FID DVA' OR a.CCC_DIVISION='FIC DVA' OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'BANK RESOURCE MANAGEMENT' THEN 'BRM' WHEN a.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' THEN 'TCM' WHEN a.CCC_DIVISION ='NON CORE' THEN 'NON CORE' WHEN (a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','EM CREDIT TRADING','FXEM MACRO TRADING','STRUCTURED RATES','LIQUID FLOW RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHER FID' WHEN (a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'IED OTHER' ELSE a.CCC_BUSINESS_AREA END, CASE WHEN a.CCC_TAPS_COMPANY = '0302' THEN 'MSIP' WHEN a.CCC_TAPS_COMPANY in ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY = '0319' THEN 'MSIM' ELSE 'OTHER' END UNION ALL SELECT 'EMEA' as Cut, a.COB_DATE, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END AS CCC_PL_REPORTING_REGION, CASE WHEN (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION = 'FID DVA' OR a.CCC_DIVISION='FIC DVA' OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'BANK RESOURCE MANAGEMENT' THEN 'BRM' WHEN a.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' THEN 'TCM' WHEN a.CCC_DIVISION ='NON CORE' THEN 'NON CORE' WHEN (a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','EM CREDIT TRADING','FXEM MACRO TRADING','STRUCTURED RATES','LIQUID FLOW RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHER FID' WHEN (a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'IED OTHER' ELSE a.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, CASE WHEN a.CCC_TAPS_COMPANY = '0302' THEN 'MSIP' WHEN a.CCC_TAPS_COMPANY in ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY = '0319' THEN 'MSIM' ELSE 'OTHER' END as CCC_TAPS_COMPANY, CASE WHEN a.PRODUCT_DESCRIPTION_DECOMP IS NULL THEN 'UNDEFINED' ELSE a.PRODUCT_DESCRIPTION_DECOMP END PRODUCT_DESCRIPTION_DECOMP, sum(a.USD_EQ_DELTA_DECOMP)/1000 as USD_DELTA FROM CDWUSER.U_DECOMP_MSR a WHERE a.COB_DATE in ('2018-02-28','2018-02-21') AND a.CCC_PL_REPORTING_REGION = 'EMEA' AND a.PRODUCT_DESCRIPTION_DECOMP IN (SELECT PRODUCT_DESCRIPTION_DECOMP FROM TOP10UKName) AND a.CCC_DIVISION <> 'FID DVA' AND a.CCC_DIVISION <> 'FIC DVA' AND CCC_STRATEGY <> 'MS DVA STR NOTES IED' GROUP BY a.COB_DATE, a.PRODUCT_DESCRIPTION_DECOMP, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END, CASE WHEN (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION = 'FID DVA' OR a.CCC_DIVISION='FIC DVA' OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'BANK RESOURCE MANAGEMENT' THEN 'BRM' WHEN a.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' THEN 'TCM' WHEN a.CCC_DIVISION ='NON CORE' THEN 'NON CORE' WHEN (a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','EM CREDIT TRADING','FXEM MACRO TRADING','STRUCTURED RATES','LIQUID FLOW RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHER FID' WHEN (a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'IED OTHER' ELSE a.CCC_BUSINESS_AREA END, CASE WHEN a.CCC_TAPS_COMPANY = '0302' THEN 'MSIP' WHEN a.CCC_TAPS_COMPANY in ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY = '0319' THEN 'MSIM' ELSE 'OTHER' END UNION ALL SELECT 'UK Group excl. CVA' as Cut, a.COB_DATE, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END AS CCC_PL_REPORTING_REGION, CASE WHEN (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION = 'FID DVA' OR a.CCC_DIVISION='FIC DVA' OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'BANK RESOURCE MANAGEMENT' THEN 'BRM' WHEN a.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' THEN 'TCM' WHEN a.CCC_DIVISION ='NON CORE' THEN 'NON CORE' WHEN (a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','EM CREDIT TRADING','FXEM MACRO TRADING','STRUCTURED RATES','LIQUID FLOW RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHER FID' WHEN (a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'IED OTHER' ELSE a.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, CASE WHEN a.CCC_TAPS_COMPANY = '0302' THEN 'MSIP' WHEN a.CCC_TAPS_COMPANY in ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY = '0319' THEN 'MSIM' ELSE 'OTHER' END as CCC_TAPS_COMPANY, CASE WHEN a.PRODUCT_DESCRIPTION_DECOMP IS NULL THEN 'UNDEFINED' ELSE a.PRODUCT_DESCRIPTION_DECOMP END PRODUCT_DESCRIPTION_DECOMP, sum(a.USD_EQ_DELTA_DECOMP)/1000 as USD_DELTA FROM CDWUSER.U_DECOMP_MSR a WHERE a.COB_DATE in ('2018-02-28','2018-02-21') AND a.LE_GROUP = 'UK' AND (a.CCC_BUSINESS_AREA NOT IN ('CPM TRADING (MPE)', 'CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') AND a.CCC_STRATEGY NOT IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) AND a.PRODUCT_DESCRIPTION_DECOMP IN (SELECT PRODUCT_DESCRIPTION_DECOMP FROM TOP10UKName) AND a.CCC_DIVISION <> 'FID DVA' AND a.CCC_DIVISION <> 'FIC DVA' AND CCC_STRATEGY <> 'MS DVA STR NOTES IED' GROUP BY a.COB_DATE, a.PRODUCT_DESCRIPTION_DECOMP, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END, CASE WHEN (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION = 'FID DVA' OR a.CCC_DIVISION='FIC DVA' OR CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'BANK RESOURCE MANAGEMENT' THEN 'BRM' WHEN a.CCC_DIVISION = 'TREASURY CAPITAL MARKETS' THEN 'TCM' WHEN a.CCC_DIVISION ='NON CORE' THEN 'NON CORE' WHEN (a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','EM CREDIT TRADING','FXEM MACRO TRADING','STRUCTURED RATES','LIQUID FLOW RATES','SECURITIZED PRODUCTS GRP','COMMODITIES','NON CORE')) THEN 'OTHER FID' WHEN (a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'IED OTHER' ELSE a.CCC_BUSINESS_AREA END, CASE WHEN a.CCC_TAPS_COMPANY = '0302' THEN 'MSIP' WHEN a.CCC_TAPS_COMPANY in ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY = '0319' THEN 'MSIM' ELSE 'OTHER' END