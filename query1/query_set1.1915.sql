select a.COB_DATE, a.CURRENCY_OF_MEASURE, a.TERM_NEW, a.TERM_BUCKET, a.CCC_LE_GROUP_BTI, a.CCC_BUSINESS_AREA, a.CCC_DIVISION, sum (a.USD_IR_UNIFIED_PV01) as PV01, CASE WHEN CURRENCY_OF_MEASURE IN ('EUR','GBP','JPY','USD') THEN CURRENCY_OF_MEASURE ELSE 'OTHER' END AS CCY, CASE WHEN a.CCC_DIVISION IN ('TREASURY CAPITAL MARKETS') AND a.CCC_PRODUCT_LINE IN ('LIQUIDITY') THEN 'TREASURY CAPITAL MARKETS-LIQUIDITY' WHEN a.CCC_DIVISION IN ('TREASURY CAPITAL MARKETS') AND a.CCC_PRODUCT_LINE IN ('L-TERM DEBT SN') THEN 'TREASURY CAPITAL MARKETS-L-TERM-DEBT' WHEN a.CCC_DIVISION IN ('TREASURY CAPITAL MARKETS') AND a.CCC_PRODUCT_LINE IN ('S-TERM DEBT') THEN 'TREASURY CAPITAL MARKETS-SHORT TERM DEBT' WHEN a.CCC_DIVISION IN ('TREASURY CAPITAL MARKETS') AND a.CCC_PRODUCT_LINE NOT IN ('LIQUIDITY','L-TERM DEBT SN','S-TERM DEBT') THEN 'TREASURY CAPITAL MARKETS-OTHER' WHEN a.CCC_DIVISION IN ('FIXED INCOME DIVISION') AND a.CCC_BUSINESS_AREA IN ('LIQUID FLOW RATES') THEN 'FID-LIQUID FLOW RATES' WHEN a.CCC_DIVISION IN ('FIXED INCOME DIVISION') AND a.CCC_BUSINESS_AREA IN ('CPM') THEN 'FID-FID CVA' WHEN a.CCC_DIVISION IN ('FIXED INCOME DIVISION') AND a.CCC_BUSINESS_AREA NOT IN ('LIQUID FLOW RATES','CPM') THEN 'FID-OTHER' WHEN a.CCC_DIVISION IN ('FID DVA') THEN 'FID DVA' WHEN a.CCC_DIVISION IN ('INSTITUTIONAL EQUITY DIVISION') THEN 'IED' ELSE 'OTHER' END AS CATEGORY from CDWUSER.U_IR_MSR_INTRPLT a WHERE a.cob_date in ('2018-02-28','2018-01-31') AND LE_GROUP = 'UK' AND a.BOOK NOT IN ('SECTD') AND CCC_STRATEGY IN ('CVA RISK MANAGEMENT','FVA RISK MANAGEMENT','CPM - OTHER','CPM CREDIT', 'CPM FUNDING','MS CVA MNE - DERIVATIVES','MS CVA MPE - DERIVATIVES') group by a.COB_DATE, a.CURRENCY_OF_MEASURE, a.TERM_NEW, a.TERM_BUCKET, a.CCC_LE_GROUP_BTI, a.CCC_BUSINESS_AREA, a.CCC_DIVISION, a.CCC_PRODUCT_LINE