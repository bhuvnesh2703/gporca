SELECT a.COB_DATE, a.TIME_BUCKET_CALENDAR, case WHEN (a.PRODUCT_TYPE_CODE in ('EAST OFF','EAST PEAK', 'GRNPWR PEAK', 'GRNPWR OFF') or a.PRODUCT_SUB_TYPE_CODE in ('NE_ISO_O', 'NY_ISO_O', 'PJM_O', 'SERC_O', 'SPP_O','NE_ISO_P', 'NY_ISO_P', 'PJM_P', 'SERC_P', 'SPP_P')) or a.PROD_POS_NAME_DESCRIPTION in ('AEP-O','COMED-O','DAYTON-O','DQE-O','KAMMER-O','NIHUB-O','SOUTHWEST-O','AEP-P','COMED-P','DAYTON-P','DQE-P','KAMMER-P','NIHUB-P','SOUTHWEST-P') then 'East' WHEN (a.PRODUCT_TYPE_CODE in ('MIDWEST OFF','MIDWEST PEAK') OR a.PRODUCT_SUB_TYPE_CODE in ('MIDWEST_P','MIDWEST_O')) AND a.PROD_POS_NAME_DESCRIPTION NOT IN ('AEP-O','COMED-O','DAYTON-O','DQE-O','KAMMER-O','NIHUB-O','SOUTHWEST-O','AEP-P','COMED-P','DAYTON-P','DQE-P','KAMMER-P','NIHUB-P','SOUTHWEST-P') then 'Midwest' WHEN a.PRODUCT_TYPE_CODE in ('TEXAS OFF','TEXAS PEAK','TEXAS INTERCONNECT O','TEXAS INTERCONNECT P', 'ERCOT') then 'Texas' WHEN a.PRODUCT_TYPE_CODE in ('WEST OFF','WEST PEAK','WEST INTERCONNECT OF','WEST INTERCONNECT PE', 'WEST OFF') then 'West' when a.PRODUCT_TYPE_CODE in ('NATGAS') then 'Natgas' when a.PRODUCT_TYPE_CODE in ('COAL', 'COAL-US', 'EMISSIONS-US','WEATHER') then 'Other Fuels' else 'other' end as region, a.PRODUCT_TYPE_CODE, sum(a.USD_CM_Delta) as dollar_delta FROM cdwuser.U_CM_MSR a WHERE ((a.CCC_DIVISION IN ('COMMODITIES') AND a.CCC_BUSINESS_AREA IN ('NA ELECTRICITYNATURAL GAS')) /*OLD LOGIC*/ OR (A.CCC_DIVISION = 'FIXED INCOME DIVISION' AND A.CCC_BUSINESS_AREA = 'COMMODITIES' and a.CCC_PRODUCT_LINE IN ('NA POWER & GAS'))) /* NEW LOGIC*/ AND A.PRODUCT_TYPE_CODE NOT IN ('CURRENCY', 'INTEREST RATE', 'INFLATION', 'TBD', 'MISC','CVA', 'FVA', 'ERROR') AND A.COB_DATE IN ('2018-02-28','2018-01-31','2017-12-29','2017-09-29','2017-06-30','2017-03-31') and NOT(a.INCLUDE_IN_REG_CAAP_FL = 'N' and a.PRODUCT_SUB_TYPE_CODE in ('N EAST CONSUMPTI', 'N EAST CONSUMPTION', 'N EAST APPALACHI', 'N EAST APPALACHIA') and a.BOOK = '18003') group by a.COB_DATE, a.PRODUCT_TYPE_CODE, a.PRODUCT_SUB_TYPE_CODE, a.PROD_POS_NAME_DESCRIPTION, a.TIME_BUCKET_CALENDAR, a.FUTURES_EQUIVALENT_DATE, a.TIME_BUCKET_QUARTER