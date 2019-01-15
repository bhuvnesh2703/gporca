SELECT A.COB_DATE, A.PRODUCT_TYPE, A.PWR_PLANT, Sum(A.DOLLAR_GREEK) as DOLLAR_GREEK, SUM(A.RAW_GREEK) as RAW_GREEK FROM (SELECT COB_Date, CCC_TRD_BOOK, BOOK, PRODUCT_TYPE_CODE, case when CCC_TRD_BOOK in ('RED OAK') then 'Red Oak' when CCC_TRD_BOOK in ('MATL') then 'MATL' when CCC_TRD_BOOK in ('STRUCTURED TRANSACTIONS') then 'Walton/Franklin/Greystone' when CCC_TRD_BOOK in ('TOPAZ TOLLING') then 'Topaz' when BOOK in ('LAPALOMA','LAPALOMA EXTENSION', 'LAPALOMA HEDGES', 'LAPALOMA EXTENSION HEDGES' ) then 'LaPaloma/Extension' else 'Other' end as pwr_plant, case when product_type_code in ('EAST OFF','EAST PEAK','MIDWEST OFF', 'MIDWEST PEAK','TEXAS OFF','TEXAS PEAK','WEST OFF','WEST PEAK','EAST INTERCONNECT OF','EAST INTERCONNECT PE','TEXAS INTERCONNECT O', 'TEXAS INTERCONNECT P', 'ERCOT', 'WEST INTERCONNECT OF', 'WEST INTERCONNECT PE') then 'Electricity' when product_type_code in ('NATGAS') then product_type_code end as product_type, sum(cast(USD_CM_Delta as numeric(15,5))) as dollar_greek, sum(cast(RAW_CM_Delta as numeric(15,5))) as raw_greek FROM cdwuser.U_CM_MSR Where COB_DATE IN ('2018-02-28','2018-02-21') AND ((CCC_BUSINESS_AREA in ('NA ELECTRICITYNATURAL GAS') AND CCC_DIVISION = 'COMMODITIES') /*OLD LOGIC*/ OR (CCC_DIVISION = 'FIXED INCOME DIVISION' AND CCC_BUSINESS_AREA = 'COMMODITIES' and CCC_PRODUCT_LINE IN ('NA POWER & GAS'))) /*NEW LOGIC*/ AND (CCC_PL_REPORTING_REGION IN ('EUROPE','EMEA')) and CCC_STRATEGY in ('NAPG EAST','NA EAST POWER','NAPG WEST','NA WEST POWER & REAL TIME') group by COB_DATE, CCC_TRD_BOOK,BOOK, PRODUCT_TYPE_CODE)A GROUP BY A.COB_DATE, A.PRODUCT_TYPE, A.PWR_PLANT