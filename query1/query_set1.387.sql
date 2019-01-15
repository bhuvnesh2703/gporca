Select a.COB_DATE, a.CCC_PL_REPORTING_REGION, a.PRODUCT_TYPE_CODE, CASE WHEN a.PRODUCT_SUB_TYPE_CODE IN ('N EAST CONSUMPTI', 'N EAST CONSUMPTION') THEN 'N EAST CONSUMPTI' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('N EAST APPALACHI', 'N EAST APPALACHIA') THEN 'N EAST APPALACHI' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('POLYPROPELENE HO', 'POLYPROPELENE HOMOPOLYMER') THEN 'POLYPROPELENE HO' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('POLYPROPYLENE CO', 'POLYPROPYLENE COPOLYMER') THEN 'POLYPROPYLENE CO' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('PURE TEREPHTHALI', 'PURE TEREPHTHALIC ACID') THEN 'PURE TEREPHTHALI' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('GSCI-PRECIOUS ME', 'GSCI-PRECIOUS METALS') THEN 'GSCI-PRECIOUS ME' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('ZERO PRICED EXPO', 'ZERO PRICED EXPOSURE') THEN 'ZERO PRICED EXPO' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('DJUBS-BASE METAL', 'DJUBS-BASE METALS') THEN 'DJUBS-BASE METAL' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('GAS SULFUR CREDI', 'GAS SULFUR CREDIT') THEN 'GAS SULFUR CREDI' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('GASOIL TIMESPREA', 'GASOIL TIMESPREAD') THEN 'GASOIL TIMESPREA' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('USD INTEREST RAT', 'USD INTEREST RATE') THEN 'USD INTEREST RAT' ELSE a.PRODUCT_SUB_TYPE_CODE END AS PRODUCT_SUB_TYPE_CODE, sum(a.raw_greek) as raw_greek, sum(a.RAW_KAPPA) as RAW_KAPPA, case when a.DAYS2exp >=1980 then '5+yrs' when (a.DAYS2exp <1980 and a.DAYS2exp >=1440)then '5yrs' when (a.DAYS2exp <1440 and a.DAYS2exp >=1080) then '4yrs' when (a.DAYS2exp <1080 and a.DAYS2exp >=720)then '3yrs' when (a.DAYS2exp <720 and a.DAYS2exp >=360) then '2yrs'when (a.DAYS2exp <360 and a.DAYS2exp >=270)then '1yr' when (a.DAYS2exp <270 and a.DAYS2exp >=180) then '9m'when (a.DAYS2exp <180 and a.DAYS2exp >=90)then '6m'when (a.DAYS2exp <90 and a.DAYS2exp >=60) then '3m' when (a.DAYS2exp <60 and a.DAYS2exp >=30)then '2m' when (a.DAYS2exp <30 and a.DAYS2exp >=7.5) then '1m'when (a.DAYS2exp <7.5 and a.DAYS2exp >=1.5)then '1wk'when a.DAYS2exp < 1.5 then'1day' else 'Check' end as MetalsTerm from ( SELECT COB_DATE,CCC_PL_REPORTING_REGION, PRODUCT_TYPE_CODE,PRODUCT_SUB_TYPE_CODE, (sum(USD_CM_LEASE_RATE)::numeric(30,10))/100 as raw_greek, sum(RAW_CM_KAPPA)::numeric(30,10) as RAW_KAPPA, EXPIRATION_DATE, (extract (YEAR from (EXPIRATION_DATE)) - extract(YEAR from (COB_DATE)))*360 + (extract(MONTH from (EXPIRATION_DATE)) - extract(MONTH from (COB_DATE)))*30+ (extract(DAY from (EXPIRATION_DATE)) -extract (DAY from (COB_DATE))) as DAYS2exp, TIME_BUCKET_CALENDAR FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-02-27') AND ((CCC_DIVISION='COMMODITIES' AND CCC_BUSINESS_AREA not in ('CREDIT','MS CVA MNE - COMMOD', 'COMMODS FINANCING','INVESTMENTS AND STR TRANS','INVESTOR BUSINESS')) OR(CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES' AND CCC_PRODUCT_LINE NOT IN ('COMMOD LENDING', 'COMMOD EXOTICS', 'COMMOD INDEX') AND CCC_STRATEGY NOT IN ('MS CVA MNE - COMMOD', 'CMD LEGACY LAONS & CLAIMS', 'CVA RISK MANAGEMENT', 'FVA RISK MANAGEMENT', 'FUNDING COLLATERAL COMM'))) /* NEW LOGIC*/ AND (CCC_PL_REPORTING_REGION IN ('EUROPE','EMEA')) and PRODUCT_TYPE_CODE in ('PRECIOUSMETAL') group by COB_DATE,CCC_PL_REPORTING_REGION, PRODUCT_TYPE_CODE, TIME_BUCKET_CALENDAR, EXPIRATION_DATE,PRODUCT_SUB_TYPE_CODE )A group by a.COB_DATE, a.CCC_PL_REPORTING_REGION, a.PRODUCT_TYPE_CODE, a.DAYS2exp, a.PRODUCT_SUB_TYPE_CODE