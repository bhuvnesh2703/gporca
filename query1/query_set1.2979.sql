select a.COB_DATE, CASE WHEN (a.CCC_DIVISION IN ('PRIVATE BANKING GROUP') AND a.PRODUCT_TYPE_CODE ='REPO' AND a.CCC_BUSINESS_AREA NOT IN ('NON CORE MARKETS','CORE MARKETS')) THEN 'BANKING' ELSE UPPER(ccc_banking_trading) END AS CAPITAL_BOOK, CASE WHEN CCC_BUSINESS_AREA='FXEM MACRO TRADING' THEN CCC_BUSINESS_AREA ELSE 'Other' END AS CCC_BUSINESS_AREA, CASE WHEN CCC_PRODUCT_LINE = 'PAR LOANS TRADING' THEN 'Par Loans Trading' ELSE 'Other' END AS CCC_PRODUCT_LINE, TERM_BUCKET, case when TERM_BUCKET_AGGR in ('5-7Y','7-10Y') then '5-10Y' else TERM_BUCKET_AGGR end as TERM_BUCKET_AGGR, a.MRD_RATING, CASE WHEN a.CCC_BUSINESS_AREA IN ('FXEM MACRO TRADING') and a.CCC_PL_REPORTING_REGION in ('AMERICAS') THEN ('US_FXEM_MACRO') ELSE 'OTHER' END AS FLAG, sum(COALESCE((USD_NOTIONAL) :: numeric(15,5),0))/1000 as USD_NOTIONAL, sum(COALESCE((USD_EXPOSURE) :: numeric(15,5), 0)) / 1000 AS USD_EXPOSURE, sum(COALESCE((USD_PROCEED) :: numeric(15,5), 0)) / 1000 AS MV, SUM(COALESCE((USD_PV01SPRD) :: numeric(15,5),0))/1000 AS USD_PV01SPRD, SUM(COALESCE((USD_IR_KAPPA) :: numeric(15,5), 0)/10) AS USD_IR_KAPPA, SUM(COALESCE((USD_CONVX) :: numeric(15,5), 0)/1000) AS USD_CONVX, SUM(COALESCE((USD_IR_UNIFIED_PV01) :: numeric(15,5), 0)) AS USD_PV01, SUM ((SLIDE_IR_PLS_100BP_USD) :: numeric(15,5)) AS SLIDE_IR_PLS_100BP_USD, SUM ((SLIDE_IR_PLS_200BP_USD) :: numeric(15,5)) AS SLIDE_IR_PLS_200BP_USD, SUM ((SLIDE_IR_PLS_300BP_USD) :: numeric(15,5)) AS SLIDE_IR_PLS_300BP_USD, SUM ((SLIDE_IR_PLS_50BP_USD) :: numeric(15,5)) AS SLIDE_IR_PLS_50BP_USD, SUM ((SLIDE_IR_MIN_100BP_USD) :: numeric(15,5)) AS SLIDE_IR_MIN_100BP_USD, SUM ((SLIDE_IR_MIN_200BP_USD) :: numeric(15,5)) AS SLIDE_IR_MIN_200BP_USD, SUM ((SLIDE_IR_MIN_300BP_USD) :: numeric(15,5)) AS SLIDE_IR_MIN_300BP_USD, sum((USD_FX) :: numeric(15,5)) AS USD_FX, sum((USD_FX_KAPPA) :: numeric(15,5)) AS USD_VEGA from cdwuser.u_dm_wm_position a WHERE A.COB_DATE in ( '2018-02-28', '2018-02-27', '2018-01-31', '2017-12-29', '2017-11-30', '2017-10-31', '2017-09-29', '2014-12-31' ) AND A.CCC_TAPS_COMPANY = '1633' AND (A.VAR_EXCL_FL<> 'Y' OR A.BOOK IN ('MSDPB3M','MSDPT3M')) and CASE WHEN a.CCC_BUSINESS_AREA IN ('FXEM MACRO TRADING') and a.CCC_PL_REPORTING_REGION in ('AMERICAS') THEN ('US_FXEM_MACRO') ELSE 'OTHER' END = 'OTHER' group by a.COB_DATE, CASE WHEN CCC_BUSINESS_AREA='FXEM MACRO TRADING' THEN CCC_BUSINESS_AREA ELSE 'Other' END, CASE WHEN CCC_PRODUCT_LINE = 'PAR LOANS TRADING' THEN 'Par Loans Trading' ELSE 'Other' END, TERM_BUCKET, case when TERM_BUCKET_AGGR in ('5-7Y','7-10Y') then '5-10Y' else TERM_BUCKET_AGGR end, a.MRD_RATING, CASE WHEN (a.CCC_DIVISION IN ('PRIVATE BANKING GROUP') AND a.PRODUCT_TYPE_CODE ='REPO' AND a.CCC_BUSINESS_AREA NOT IN ('NON CORE MARKETS','CORE MARKETS')) THEN 'BANKING' ELSE UPPER(ccc_banking_trading) END, CASE WHEN a.CCC_BUSINESS_AREA IN ('FXEM MACRO TRADING') and a.CCC_PL_REPORTING_REGION in ('AMERICAS') THEN ('US_FXEM_MACRO') ELSE 'OTHER' END