SELECT COB_DATE, a.CCC_BUSINESS_AREA, a.CCC_PRODUCT_LINE, a.CCC_STRATEGY, a.TIME_BUCKET_CALENDAR, CASE WHEN CCC_PRODUCT_LINE = 'EU POWER & GAS' THEN 'EU POWER & GAS' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) AND a.PRODUCT_TYPE_CODE = 'FUEL' then 'Fuel Oil' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) AND A.PRODUCT_TYPE_CODE = 'JET' THEN 'JET FUEL' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) AND a.PRODUCT_TYPE_CODE = 'DIST' then 'Mid-Distillates' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) and ( a.PRODUCT_TYPE_CODE = 'CRUDE' OR (a.PRODUCT_TYPE_CODE = 'TIMESPREAD' and a.PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD') ) then 'Crude' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) AND A.PRODUCT_TYPE_CODE IN ( 'GAS','NAPHTHA', 'NGL', 'ETHANOL') then 'Gasoline & Related' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) then 'Other Products' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) AND ( a.PRODUCT_TYPE_CODE in ('EAST OFF','EAST PEAK', 'GRNPWR PEAK', 'GRNPWR OFF') or a.PRODUCT_SUB_TYPE_CODE in ('NE_ISO_O', 'NY_ISO_O', 'PJM_O', 'SERC_O', 'SPP_O','NE_ISO_P', 'NY_ISO_P', 'PJM_P', 'SERC_P', 'SPP_P') or a.PROD_POS_NAME_DESCRIPTION in ('AEP-O','COMED-O','DAYTON-O','DQE-O','KAMMER-O','NIHUB-O','SOUTHWEST-O','AEP-P','COMED-P','DAYTON-P','DQE-P','KAMMER-P','NIHUB-P','SOUTHWEST-P') ) then 'East - NA' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) AND ( a.PRODUCT_TYPE_CODE in ('MIDWEST OFF','MIDWEST PEAK') OR a.PRODUCT_SUB_TYPE_CODE in ('MIDWEST_P','MIDWEST_O') ) AND a.PROD_POS_NAME_DESCRIPTION NOT IN ('AEP-O','COMED-O','DAYTON-O','DQE-O','KAMMER-O','NIHUB-O','SOUTHWEST-O','AEP-P','COMED-P','DAYTON-P','DQE-P','KAMMER-P','NIHUB-P','SOUTHWEST-P') then 'MIDWEST - NA' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('TEXAS OFF','TEXAS PEAK','TEXAS INTERCONNECT O','TEXAS INTERCONNECT P', 'ERCOT') then 'TEXAS - NA' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('WEST OFF','WEST PEAK','WEST INTERCONNECT OF','WEST INTERCONNECT PE', 'WEST OFF') then 'WEST - NA' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('NATGAS') and a.CCC_STRATEGY IN ('NAPG NAT GAS','NA NATURAL GAS') then 'Nat Gas Desk' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('NATGAS') and a.CCC_STRATEGY NOT IN ('NAPG NAT GAS','NA NATURAL GAS') then 'Power Desk' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('EMISSIONS-US') then 'Emissions - US' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) then 'OTHER NAPG' WHEN (A.CCC_BUSINESS_AREA = 'METALS' and A.PRODUCT_TYPE_CODE in ('BASEMETAL', 'COAL','COAL-US',/* 'DRY FREIGHT',*/ 'EMISSIONS-EU','IRON ORE')) /*OLD LOGIC*/ THEN 'Legacy Trading' WHEN ((a.ccc_business_area IN ('AGRICULTURALS', 'AP EU ELECTRICNATURAL GAS', 'OLYMPUS', 'TMG')) or (a.CCC_PRODUCT_LINE in ('LEGACY OIL'))) /* NEW LOGIC*/ THEN 'Legacy Trading' WHEN (A.CCC_PRODUCT_LINE = 'COMMOD LEGACY TRADING') /* NEW LOGIC*/ THEN 'Legacy Trading' WHEN ((a.CCC_BUSINESS_AREA in ('INVESTOR BUSINESS') and a.CCC_PRODUCT_LINE IN ('IB INDEX')) /*OLD LOGIC*/ OR (A.CCC_PRODUCT_LINE IN ('COMMOD INDEX'))) /* NEW LOGIC*/ THEN 'Index' WHEN ((a.CCC_BUSINESS_AREA in ('INVESTOR BUSINESS') and a.CCC_PRODUCT_LINE IN ('IB STRUCTURED')) /*OLD LOGIC*/ OR (A.CCC_PRODUCT_LINE IN ('COMMOD EXOTICS'))) /* NEW LOGIC*/ THEN 'Exotics' WHEN ((A.CCC_BUSINESS_AREA = 'METALS' and A.PRODUCT_TYPE_CODE in ('PRECIOUSMETAL', 'FUND')) /*OLD LOGIC*/or (a.CCC_PRODUCT_LINE in ('PRECIOUS METALS'))) /* NEW LOGIC*/ then 'Precious Metals' WHEN (A.CCC_BUSINESS_AREA = 'COMMODS FINANCING' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE IN ('COMMOD - FUNDING', 'COMMOD LENDING') /* NEW LOGIC*/) THEN 'XVA/LENDING' else 'Other' end as GROUPED_CM_BREAKDOWN, a.PRODUCT_TYPE_CODE, EXPIRATION_DATE, case when EXPIRATION_DATE < '2019-01-01' then time_bucket_quarter end as quarters, CASE WHEN PRODUCT_TYPE_CODE in ('JET', 'LNG', 'MIDWEST OFF', 'MIDWEST PEAK', 'NAPTHA', 'NAPHTHA','NATGAS', 'NGL', ' PETCHEM', 'DIST', 'WEST OFF', 'WEST PEAK', 'ETHANAL', 'EUR NG', 'FUEL', 'GASOLINE CREDITS', 'GAS', /* 'DRY FREIGHT',*/ 'COAL', 'CRUDE', 'EMISSIONS-EU') THEN 'Energy' WHEN (CCC_PRODUCT_LINE in ('COMMOD INDEX','COMMOD EXOTICS') and PRODUCT_TYPE_CODE in ( 'PRECIOUSMETAL','BASEMETAL', 'GRAINS', 'GSCI', 'INDEX', 'SOFT / LIVESTOCK','PLASTICS', 'VEG OILS', 'EQUITY INDEX')) OR (CCC_BUSINESS_AREA in ('INVESTOR BUSINESS') and PRODUCT_TYPE_CODE in ( 'PRECIOUSMETAL','BASEMETAL', 'GRAINS', 'GSCI', 'INDEX', 'SOFT/LIVESTOCK','PLASTICS', 'VEG OILS', 'EQUITY INDEX')) THEN 'Non-Energy' END AS ENERGY_BREAKDOWN, SUM ((USD_CM_DELTA)) AS DOLLAR_GREEK, SUM (CASE WHEN a.PRODUCT_TYPE_CODE = 'TIMESPREAD' AND a.PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD' THEN 0 WHEN a.book = 'COMXO' and A.CCC_PRODUCT_LINE IN ('COMMOD EXOTICS') then CAST(USD_CM_KAPPA_ABSOLUTE AS NUMERIC (15, 5)) ELSE CAST (a.RAW_CM_KAPPA AS NUMERIC (15, 5)) END) AS RAW_GREEK FROM CDWUSER.U_CM_MSR a WHERE NOT(INCLUDE_IN_REG_CAAP_FL = 'N' and PRODUCT_SUB_TYPE_CODE in ('N EAST CONSUMPTI', 'N EAST CONSUMPTION', 'N EAST APPALACHI', 'N EAST APPALACHIA') and BOOK = '18003') AND ((CCC_BUSINESS_AREA NOT IN ('CREDIT', 'MS CVA MNE - COMMOD') AND CCC_DIVISION = 'COMMODITIES' ) /*OLD LOGIC*/ OR (A.CCC_DIVISION = 'FIXED INCOME DIVISION' AND A.CCC_BUSINESS_AREA = 'COMMODITIES' and a.CCC_STRATEGY NOT IN ('MS CVA MNE - COMMOD'))) /*NEW LOGIC; THERE IS NO CREDIT BUSINESS AREA IN CM ANYMORE CALLED CREDIT*/ AND PRODUCT_TYPE_CODE NOT IN ('CURRENCY', 'INTEREST RATE', 'INFLATION', 'TBD', 'MISC','CVA', 'FVA', 'ERROR') AND NOT(cob_date >= '2017-03-29' and cob_date <= '2017-07-05' and A.CCC_PRODUCT_LINE in( 'IB STRUCTURED', 'COMMOD EXOTICS') AND A.PRODUCT_TYPE_CODE = 'ZCS') /* Removed: AND CCC_TRD_BOOK <> 'DEVELOPMENT' */ and cob_date in ('2018-02-28', '2018-02-27', '2017-12-29', '2017-09-29', '2017-06-30', '2017-03-31', '2016-12-30', '2016-09-30') AND LE_GROUP = 'UK' GROUP BY COB_DATE, a.CCC_BUSINESS_AREA, a.CCC_PRODUCT_LINE, a.CCC_STRATEGY, a.TIME_BUCKET_CALENDAR, CASE WHEN CCC_PRODUCT_LINE = 'EU POWER & GAS' THEN 'EU POWER & GAS' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) AND a.PRODUCT_TYPE_CODE = 'FUEL' then 'Fuel Oil' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) AND A.PRODUCT_TYPE_CODE = 'JET' THEN 'JET FUEL' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) AND a.PRODUCT_TYPE_CODE = 'DIST' then 'Mid-Distillates' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) and ( a.PRODUCT_TYPE_CODE = 'CRUDE' OR (a.PRODUCT_TYPE_CODE = 'TIMESPREAD' and a.PROD_POS_NAME_DESCRIPTION = 'WCS POST SPREAD') ) then 'Crude' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) AND A.PRODUCT_TYPE_CODE IN ( 'GAS','NAPHTHA', 'NGL', 'ETHANOL') then 'Gasoline & Related' WHEN (a.CCC_BUSINESS_AREA = 'OIL LIQUIDS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'OIL & PRODUCTS' /* NEW LOGIC*/) then 'Other Products' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) AND ( a.PRODUCT_TYPE_CODE in ('EAST OFF','EAST PEAK', 'GRNPWR PEAK', 'GRNPWR OFF') or a.PRODUCT_SUB_TYPE_CODE in ('NE_ISO_O', 'NY_ISO_O', 'PJM_O', 'SERC_O', 'SPP_O','NE_ISO_P', 'NY_ISO_P', 'PJM_P', 'SERC_P', 'SPP_P') or a.PROD_POS_NAME_DESCRIPTION in ('AEP-O','COMED-O','DAYTON-O','DQE-O','KAMMER-O','NIHUB-O','SOUTHWEST-O','AEP-P','COMED-P','DAYTON-P','DQE-P','KAMMER-P','NIHUB-P','SOUTHWEST-P') ) then 'East - NA' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) AND ( a.PRODUCT_TYPE_CODE in ('MIDWEST OFF','MIDWEST PEAK') OR a.PRODUCT_SUB_TYPE_CODE in ('MIDWEST_P','MIDWEST_O') ) AND a.PROD_POS_NAME_DESCRIPTION NOT IN ('AEP-O','COMED-O','DAYTON-O','DQE-O','KAMMER-O','NIHUB-O','SOUTHWEST-O','AEP-P','COMED-P','DAYTON-P','DQE-P','KAMMER-P','NIHUB-P','SOUTHWEST-P') then 'MIDWEST - NA' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('TEXAS OFF','TEXAS PEAK','TEXAS INTERCONNECT O','TEXAS INTERCONNECT P', 'ERCOT') then 'TEXAS - NA' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('WEST OFF','WEST PEAK','WEST INTERCONNECT OF','WEST INTERCONNECT PE', 'WEST OFF') then 'WEST - NA' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('NATGAS') and a.CCC_STRATEGY IN ('NAPG NAT GAS','NA NATURAL GAS') then 'Nat Gas Desk' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('NATGAS') and a.CCC_STRATEGY NOT IN ('NAPG NAT GAS','NA NATURAL GAS') then 'Power Desk' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) and a.PRODUCT_TYPE_CODE in ('EMISSIONS-US') then 'Emissions - US' WHEN (a.CCC_BUSINESS_AREA ='NA ELECTRICITYNATURAL GAS' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE = 'NA POWER & GAS' /* NEW LOGIC*/) then 'OTHER NAPG' WHEN (A.CCC_BUSINESS_AREA = 'METALS' and A.PRODUCT_TYPE_CODE in ('BASEMETAL', 'COAL','COAL-US',/* 'DRY FREIGHT',*/ 'EMISSIONS-EU','IRON ORE')) /*OLD LOGIC*/ THEN 'Legacy Trading' WHEN ((a.ccc_business_area IN ('AGRICULTURALS', 'AP EU ELECTRICNATURAL GAS', 'OLYMPUS', 'TMG')) or (a.CCC_PRODUCT_LINE in ('LEGACY OIL'))) /* NEW LOGIC*/ THEN 'Legacy Trading' WHEN (A.CCC_PRODUCT_LINE = 'COMMOD LEGACY TRADING') /* NEW LOGIC*/ THEN 'Legacy Trading' WHEN ((a.CCC_BUSINESS_AREA in ('INVESTOR BUSINESS') and a.CCC_PRODUCT_LINE IN ('IB INDEX')) /*OLD LOGIC*/ OR (A.CCC_PRODUCT_LINE IN ('COMMOD INDEX'))) /* NEW LOGIC*/ THEN 'Index' WHEN ((a.CCC_BUSINESS_AREA in ('INVESTOR BUSINESS') and a.CCC_PRODUCT_LINE IN ('IB STRUCTURED')) /*OLD LOGIC*/ OR (A.CCC_PRODUCT_LINE IN ('COMMOD EXOTICS'))) /* NEW LOGIC*/ THEN 'Exotics' WHEN ((A.CCC_BUSINESS_AREA = 'METALS' and A.PRODUCT_TYPE_CODE in ('PRECIOUSMETAL', 'FUND')) /*OLD LOGIC*/or (a.CCC_PRODUCT_LINE in ('PRECIOUS METALS'))) /* NEW LOGIC*/ then 'Precious Metals' WHEN (A.CCC_BUSINESS_AREA = 'COMMODS FINANCING' /*OLD LOGIC*/ OR A.CCC_PRODUCT_LINE IN ('COMMOD - FUNDING', 'COMMOD LENDING') /* NEW LOGIC*/) THEN 'XVA/LENDING' else 'Other' end, a.PRODUCT_TYPE_CODE, EXPIRATION_DATE, case when EXPIRATION_DATE < '2019-01-01' then time_bucket_quarter end, CASE WHEN PRODUCT_TYPE_CODE in ('JET', 'LNG', 'MIDWEST OFF', 'MIDWEST PEAK', 'NAPTHA', 'NAPHTHA','NATGAS', 'NGL', ' PETCHEM', 'DIST', 'WEST OFF', 'WEST PEAK', 'ETHANAL', 'EUR NG', 'FUEL', 'GASOLINE CREDITS', 'GAS', /* 'DRY FREIGHT',*/ 'COAL', 'CRUDE', 'EMISSIONS-EU') THEN 'Energy' WHEN (CCC_PRODUCT_LINE in ('COMMOD INDEX','COMMOD EXOTICS') and PRODUCT_TYPE_CODE in ( 'PRECIOUSMETAL','BASEMETAL', 'GRAINS', 'GSCI', 'INDEX', 'SOFT / LIVESTOCK','PLASTICS', 'VEG OILS', 'EQUITY INDEX')) OR (CCC_BUSINESS_AREA in ('INVESTOR BUSINESS') and PRODUCT_TYPE_CODE in ( 'PRECIOUSMETAL','BASEMETAL', 'GRAINS', 'GSCI', 'INDEX', 'SOFT/LIVESTOCK','PLASTICS', 'VEG OILS', 'EQUITY INDEX')) THEN 'Non-Energy' END