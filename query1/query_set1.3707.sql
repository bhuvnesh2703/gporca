SELECT a.COB_DATE, CASE WHEN a.CURRENCY_COMBINED IN ('CNH') THEN ('CNY') WHEN a.CURRENCY_COMBINED IN ('BRD') THEN ('BRL') WHEN a.CURRENCY_COMBINED IN ('BHD','QAR','KWD','SAR','AED','OMR','BGN') THEN 'OTHER_PEGGED' ELSE a.CURRENCY_COMBINED END AS CURRENCY, a.neg20, a.pos20, CASE WHEN (a.CCC_BUSINESS_AREA IN ( 'CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES', 'EQ XVA HEDGING')) THEN 'CVA' WHEN (a.CCC_DIVISION IN ('FID DVA', 'FIC DVA') OR a.CCC_STRATEGY = 'MS DVA STR NOTES IED') THEN 'DVA' WHEN CCC_DIVISION = 'COMMODITIES' THEN 'FIXED INCOME DIVISION' ELSE CCC_DIVISION END AS CCC_DIVISION, a.IS_UK_GROUP, a.CCC_PL_REPORTING_REGION, CASE WHEN a.CCC_DIVISION = 'COMMODITIES' THEN 'COMMODITIES' WHEN (a.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CASH EQUITIES','DERIVATIVES','OTHER IED','PRIME BROKERAGE')) THEN 'OTHERS' WHEN (a.CCC_DIVISION='FIXED INCOME DIVISION' AND a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','FXEM MACRO TRADING','EM CREDIT TRADING','LIQUID FLOW RATES','STRUCTURED RATES','SECURITIZED PRODUCTS GRP','COMMODITIES')) THEN 'OTHERS FID' ELSE a.CCC_BUSINESS_AREA END AS CCC_BUSINESS_AREA, a.entityclassification FROM ( SELECT V.COB_DATE, V.CCC_DIVISION, V.CCC_BUSINESS_AREA, V.CCC_STRATEGY, CASE WHEN V.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN V.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN V.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END AS CCC_PL_REPORTING_REGION, V.IS_UK_GROUP, CASE WHEN V.base_ccy = 'UNDEFINED' THEN CURRENCY_OF_RISK_CCY1 ELSE V.base_ccy END AS currency_COMBINED, SUM (V.SLIDE_FXOPTVAR_MIN_20PCT_USD) AS neg20, SUM (V.SLIDE_FXOPTVAR_PLS_20PCT_USD) AS pos20, CASE WHEN V.CCC_TAPS_COMPANY='0319' THEN 'MSIM' WHEN V.CCC_TAPS_COMPANY IN ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN V.CCC_TAPS_COMPANY='0302' THEN 'MSIP' WHEN V.IS_UK_GROUP = 'Y' THEN 'OtherUKG' ELSE 'NOTUKG' END AS entityclassification FROM cdwuser.U_DM_FX V WHERE V.cob_date IN ('2018-02-28','2018-02-21') AND ((V.CURRENCY_PAIR LIKE '%AUD%' AND V.risk_currency_combined = 'AUD') OR (V.CURRENCY_PAIR LIKE '%ZAR%' AND V.risk_currency_combined = 'ZAR') OR (V.CURRENCY_PAIR LIKE '%EUR%' AND V.risk_currency_combined = 'EUR') OR (V.CURRENCY_PAIR LIKE '%CHF%' AND V.risk_currency_combined = 'CHF') OR (V.CURRENCY_PAIR LIKE '%JPY%' AND V.risk_currency_combined = 'JPY') OR (V.CURRENCY_PAIR LIKE '%GBP%' AND V.risk_currency_combined = 'GBP') OR (V.CURRENCY_PAIR LIKE '%NZD%' AND V.risk_currency_combined = 'NZD') OR (V.CURRENCY_PAIR LIKE '%CAD%' AND V.risk_currency_combined = 'CAD') OR (V.CURRENCY_PAIR LIKE '%SEK%' AND V.risk_currency_combined = 'SEK') OR (V.CURRENCY_PAIR LIKE '%NOK%' AND V.risk_currency_combined = 'NOK') OR (V.CURRENCY_PAIR LIKE '%DKK%' AND V.risk_currency_combined = 'DKK') OR ((V.CURRENCY_PAIR LIKE '%RUB%' OR V.CURRENCY_PAIR LIKE '%RBX%' OR V.CURRENCY_PAIR LIKE '%RU1%') AND V.risk_currency_combined = 'RUB') OR (V.CURRENCY_PAIR LIKE '%TRY%' AND V.risk_currency_combined = 'TRY') OR ((V.CURRENCY_PAIR LIKE '%CNY%' OR V.CURRENCY_PAIR LIKE '%CNH%') AND V.risk_currency_combined = 'CNY') OR (V.CURRENCY_PAIR LIKE '%INR%' AND V.risk_currency_combined = 'INR') OR ((V.CURRENCY_PAIR LIKE '%KRW%' OR v.CURRENCY_PAIR LIKE '%KRX%') AND V.risk_currency_combined = 'KRW') OR ((V.CURRENCY_PAIR LIKE '%BRL%' OR V.CURRENCY_PAIR LIKE '%BRX%' OR V.CURRENCY_PAIR LIKE '%BR1%') AND V.risk_currency_combined = 'BRL') OR (V.CURRENCY_PAIR LIKE '%MXN%' AND V.risk_currency_combined = 'MXN') OR (V.CURRENCY_PAIR LIKE '%HKD%' AND V.risk_currency_combined = 'HKD') OR (V.CURRENCY_PAIR LIKE '%CZK%' AND V.risk_currency_combined = 'CZK') OR (V.CURRENCY_PAIR LIKE '%THB%' AND V.risk_currency_combined = 'THB')) AND V.vertical_system LIKE '%FXOPT%' AND V.PRODUCT_TYPE_CODE = 'FXOPT' AND V.is_basketbook = 'N' GROUP BY V.COB_DATE, V.CCC_DIVISION, V.CCC_BUSINESS_AREA, V.CCC_STRATEGY, CASE WHEN V.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN V.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' WHEN V.CCC_PL_REPORTING_REGION IN ('ASIA PACIFIC','JAPAN') THEN 'ASIA' ELSE 'OTHER' END, V.IS_UK_GROUP, CASE WHEN V.base_ccy = 'UNDEFINED' THEN CURRENCY_OF_RISK_CCY1 ELSE V.base_ccy END, CASE WHEN V.CCC_TAPS_COMPANY='0319' THEN 'MSIM' WHEN V.CCC_TAPS_COMPANY IN ('4663','7281','5274','5254','8179','7280','6262','1311','8292','0721','4391','0517') THEN 'MSBIL' WHEN V.CCC_TAPS_COMPANY='0302' THEN 'MSIP' WHEN V.IS_UK_GROUP = 'Y' THEN 'OtherUKG' ELSE 'NOTUKG' END ) a