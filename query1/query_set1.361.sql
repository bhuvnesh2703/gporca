Select case when CCC_BUSINESS_AREA = 'COMMODITIES' then CCC_PRODUCT_LINE else CCC_BUSINESS_AREA end as CCC_BUSINESS_AREA, A.COB_DATE, A.CCC_PL_REPORTING_REGION, A.PHYS, CASE WHEN A.PRODUCT_SUB_TYPE_CODE IN ('N EAST CONSUMPTI', 'N EAST CONSUMPTION') THEN 'N EAST CONSUMPTI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('N EAST APPALACHI', 'N EAST APPALACHIA') THEN 'N EAST APPALACHI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('POLYPROPELENE HO', 'POLYPROPELENE HOMOPOLYMER') THEN 'POLYPROPELENE HO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('POLYPROPYLENE CO', 'POLYPROPYLENE COPOLYMER') THEN 'POLYPROPYLENE CO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('PURE TEREPHTHALI', 'PURE TEREPHTHALIC ACID') THEN 'PURE TEREPHTHALI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GSCI-PRECIOUS ME', 'GSCI-PRECIOUS METALS') THEN 'GSCI-PRECIOUS ME' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('ZERO PRICED EXPO', 'ZERO PRICED EXPOSURE') THEN 'ZERO PRICED EXPO' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('DJUBS-BASE METAL', 'DJUBS-BASE METALS') THEN 'DJUBS-BASE METAL' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GAS SULFUR CREDI', 'GAS SULFUR CREDIT') THEN 'GAS SULFUR CREDI' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('GASOIL TIMESPREA', 'GASOIL TIMESPREAD') THEN 'GASOIL TIMESPREA' WHEN A.PRODUCT_SUB_TYPE_CODE IN ('USD INTEREST RAT', 'USD INTEREST RATE') THEN 'USD INTEREST RAT' ELSE PRODUCT_SUB_TYPE_CODE END AS PRODUCT_SUB_TYPE_CODE, A.QUARTERS, A.TIME_BUCKET_CALENDAR, SUM(A.RAW_GREEK) as RAW_GREEK, SUM(A.DOLLAR_GREEK) as DOLLAR_GREEK From (select prod_pos_name_description, CCC_PRODUCT_LINE,product_sub_type_code, product_type_code, CMDTY_CD,EXPIRATION_DATE,time_bucket_quarter,CCC_TRD_BOOK, product_sub_type_name,COB_DATE,CCC_BUSINESS_AREA,TIME_BUCKET_CALENDAR,CCC_STRATEGY,CCC_PL_REPORTING_REGION, sum(cast(USD_CM_Delta as numeric(15,5))) as dollar_greek, sum(cast(RAW_CM_Delta as numeric(15,5))) as raw_greek, case when product_type_code='COAL' then 'COAL' when product_sub_type_name in ('PHYSICAL', 'SWAPTION') then 'phys' when prod_pos_name_description like 'EUA%' and product_type_code='CO2' then 'eua' when product_type_code='CO2' then prod_pos_name_description when product_sub_type_name like 'SPREAD EUROPEAN' then 'Gas Capacity Option' when product_sub_type_name like 'ELECTRICITY TRANSMISSION%' then 'Electricity Transmission Option' end as PHYS, case when product_type_code in ('CURRENCY','INTEREST RATE') then 'IR_Ccy' else 'x_IR_Ccy' end as IR_Ccy, case when EXPIRATION_DATE < ('2019-09-01') then time_bucket_quarter end as quarters, case when CCC_BUSINESS_AREA = 'METALS' AND product_type_code in ('BASEMETAL','PRECIOUSMETAL', 'FUND') then 'METAL' when product_type_code in ('GRAINS', 'COFFEE/COCOA', 'SUGAR', 'VEG OILS', 'LIVESTOCK', 'SOFT', 'COTTON', 'SOFT / LIVESTOCK') then 'Soft' when product_type_code in ('EAST OFF', 'EAST PEAK','MIDWEST OFF', 'MIDWEST PEAK','TEXAS OFF', 'TEXAS PEAK', 'WEST OFF', 'WEST PEAK','EURO PWR') then 'ELECTRICITY' when product_type_code in ('NATGAS','EUR NG') then 'NATGAS' when product_type_code in ('CRUDE','DIST','FUEL','GAS','JET','NAPHTHA', 'ETHANOL', 'NGL','CLEAN FREIGHT','DIRTY FREIGHT') or CMDTY_CD='SPREADS' then 'OIL LIQUIDS' when product_type_code in('DRY FREIGHT','COAL') then product_type_code end as product_type FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-02-27') AND ((CCC_DIVISION='COMMODITIES' AND CCC_BUSINESS_AREA not in ('CREDIT','MS CVA MNE - COMMOD', 'COMMODS FINANCING','INVESTMENTS AND STR TRANS','INVESTOR BUSINESS')) OR(CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES' AND CCC_PRODUCT_LINE NOT IN ('COMMOD LENDING', 'COMMOD EXOTICS', 'COMMOD INDEX') AND CCC_STRATEGY NOT IN ('MS CVA MNE - COMMOD', 'CMD LEGACY LAONS & CLAIMS', 'CVA RISK MANAGEMENT', 'FVA RISK MANAGEMENT', 'FUNDING COLLATERAL COMM'))) /* NEW LOGIC*/ AND (CCC_PL_REPORTING_REGION IN ('EUROPE','EMEA')) and product_type_code not in ('TBD','MISC') group by prod_pos_name_description, CCC_PRODUCT_LINE,product_sub_type_code,product_type_code,product_sub_type_name,COB_DATE,CCC_PL_REPORTING_REGION, CCC_BUSINESS_AREA,TIME_BUCKET_CALENDAR, CMDTY_CD, EXPIRATION_DATE,time_bucket_quarter,CCC_TRD_BOOK,CCC_STRATEGY)A GROUP by A.CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, A.COB_DATE, A.CCC_PL_REPORTING_REGION, PHYS, A.PRODUCT_SUB_TYPE_CODE, A.QUARTERS, A.TIME_BUCKET_CALENDAR