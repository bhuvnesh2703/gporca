Select COB_DATE, CCC_TAPS_COMPANY, CASE WHEN (CCC_BUSINESS_AREA IN ('NA ELECTRICITYNATURAL GAS') OR CCC_PRODUCT_LINE IN ('NA POWER & GAS')) THEN 'NA ELECTRICITYNATURAL GAS' WHEN ((CCC_BUSINESS_AREA in ('OIL LIQUIDS') AND CCC_PRODUCT_LINE NOT IN ('LEGACY OIL')) OR CCC_PRODUCT_LINE IN ('OIL & PRODUCTS')) THEN 'OIL LIQUIDS' WHEN ((CCC_BUSINESS_AREA IN ('METALS') AND CCC_PRODUCT_LINE NOT IN ('BULKS', 'BASE METALS')) OR CCC_PRODUCT_LINE IN ('PRECIOUS METALS')) THEN 'PRECIOUS METALS' WHEN (CCC_BUSINESS_AREA IN ('INVESTOR BUSINESS') OR CCC_PRODUCT_LINE IN ('COMMOD EXOTICS', 'COMMOD INDEX')) THEN 'INVESTOR BUSINESS' WHEN ((CCC_BUSINESS_AREA IN ('AGRICULTURALS','AP EU ELECRICNATURAL GAS', 'OLYMPUS','TMG')) OR (CCC_BUSINESS_AREA IN ('METALS') AND CCC_PRODUCT_LINE IN ('BULKS', 'BASE METALS')) OR (CCC_PRODUCT_LINE IN ('COMMOD LEGACY TRADING'))) THEN 'LEGACY TRADING' WHEN (CCC_BUSINESS_AREA IN ('COMMOND - FUNDING')) THEN 'COMMOD - FUNDING' ELSE 'OTHER' END AS CCC_BUSINESS_AREA, SUM(cast(USD_CM_LEASE_RATE as numeric(15,5))) as USD_PV01, SUM(cast(USD_CM_DELTA as numeric(15,5))) as DELTA, SUM(cast(USD_CM_KAPPA as numeric(15,5))) as KAPPA, SUM(cast(USD_CM_GAMMA/20 as numeric(15,5))) as GAMMA FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-02-27') AND ((CCC_DIVISION IN ('COMMODITIES')) /*OLD LOGIC*/ OR (CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES')) /*NEW LOGIC*/ GROUP BY COB_DATE, CCC_TAPS_COMPANY, CASE WHEN (CCC_BUSINESS_AREA IN ('NA ELECTRICITYNATURAL GAS') OR CCC_PRODUCT_LINE IN ('NA POWER & GAS')) THEN 'NA ELECTRICITYNATURAL GAS' WHEN ((CCC_BUSINESS_AREA in ('OIL LIQUIDS') AND CCC_PRODUCT_LINE NOT IN ('LEGACY OIL')) OR CCC_PRODUCT_LINE IN ('OIL & PRODUCTS')) THEN 'OIL LIQUIDS' WHEN ((CCC_BUSINESS_AREA IN ('METALS') AND CCC_PRODUCT_LINE NOT IN ('BULKS', 'BASE METALS')) OR CCC_PRODUCT_LINE IN ('PRECIOUS METALS')) THEN 'PRECIOUS METALS' WHEN (CCC_BUSINESS_AREA IN ('INVESTOR BUSINESS') OR CCC_PRODUCT_LINE IN ('COMMOD EXOTICS', 'COMMOD INDEX')) THEN 'INVESTOR BUSINESS' WHEN ((CCC_BUSINESS_AREA IN ('AGRICULTURALS','AP EU ELECRICNATURAL GAS', 'OLYMPUS','TMG')) OR (CCC_BUSINESS_AREA IN ('METALS') AND CCC_PRODUCT_LINE IN ('BULKS', 'BASE METALS')) OR (CCC_PRODUCT_LINE IN ('COMMOD LEGACY TRADING'))) THEN 'LEGACY TRADING' WHEN (CCC_BUSINESS_AREA IN ('COMMOND - FUNDING')) THEN 'COMMOD - FUNDING' ELSE 'OTHER' END