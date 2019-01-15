SELECT a.COB_DATE, SUM (a.USD_BREAKEVEN_KAPPA/10) AS usd_breakeven_kappa, CASE WHEN a.CURRENCY_OF_MEASURE IN ('USD', 'EUR', 'GBP', 'JPY') THEN CURRENCY_OF_MEASURE WHEN a.CURRENCY_OF_MEASURE IN ('AUD', 'CAD', 'CHF', 'DKK', 'NOK', 'NZD', 'SEK') THEN 'Others' ELSE 'OthersEM' END AS CURRENCY_CODE1, CASE WHEN (a.CCC_BUSINESS_AREA IN ( 'CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION IN ('FID DVA', 'FIC DVA') OR a.CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'FIXED INCOME DIVISION' ELSE a.CCC_DIVISION END AS CCC_DIVISION, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END AS CCC_PL_REPORTING_REGION, CASE WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'COMMODITIES' WHEN (a.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'OTHERS' WHEN (a.CCC_DIVISION='FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','FXEM MACRO TRADING','EM CREDIT TRADING','LIQUID FLOW RATES','STRUCTURED RATES','SECURITIZED PRODUCTS GRP','COMMODITIES')) THEN 'OTHERS FID' ELSE a.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, CASE WHEN a.CCC_TAPS_COMPANY='0319' THEN 'MSIM' WHEN a.CCC_TAPS_COMPANY IN ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY='0302' THEN 'MSIP' WHEN (a.LE_GROUP = 'UK' AND a.CCC_TAPS_COMPANY NOT IN ('0319','0517','0302','4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391')) THEN 'OtherUKG' ELSE 'NOTUKG' END AS entityclassification FROM cdwuser.U_IR_MSR a WHERE a.cob_date IN ('2018-02-28','2018-02-27') AND a.USD_BREAKEVEN_KAPPA IS NOT NULL GROUP BY a.COB_DATE, CASE WHEN a.CURRENCY_OF_MEASURE IN ('USD', 'EUR', 'GBP', 'JPY') THEN CURRENCY_OF_MEASURE WHEN a.CURRENCY_OF_MEASURE IN ('AUD', 'CAD', 'CHF', 'DKK', 'NOK', 'NZD', 'SEK') THEN 'Others' ELSE 'OthersEM' END, CASE WHEN (a.CCC_BUSINESS_AREA IN ( 'CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION IN ('FID DVA', 'FIC DVA') OR a.CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'FIXED INCOME DIVISION' ELSE a.CCC_DIVISION END, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN a.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END, CASE WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'COMMODITIES' WHEN (a.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'OTHERS' WHEN (a.CCC_DIVISION='FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','FXEM MACRO TRADING','EM CREDIT TRADING','LIQUID FLOW RATES','STRUCTURED RATES','SECURITIZED PRODUCTS GRP','COMMODITIES')) THEN 'OTHERS FID' ELSE a.CCC_BUSINESS_AREA END, CASE WHEN a.CCC_TAPS_COMPANY='0319' THEN 'MSIM' WHEN a.CCC_TAPS_COMPANY IN ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN a.CCC_TAPS_COMPANY='0302' THEN 'MSIP' WHEN (a.LE_GROUP = 'UK' AND a.CCC_TAPS_COMPANY NOT IN ('0319','0517','0302','4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391')) THEN 'OtherUKG' ELSE 'NOTUKG' END