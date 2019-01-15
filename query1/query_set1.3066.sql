select a.COB_DATE, a.CCC_TAPS_COMPANY, a.FACILITY_TYPE, a.BOOK AS RESI_MTG_BOOK, Round(a.COUPON/.5,0)*.5 as COUPON, case when COUPON <=5 then round(COUPON/.5,0)*.5 else round(COUPON,0) ::numeric(15,5) end as COUPON2, a.VINTAGE_YEAR, a.VINTAGE, SUM(COALESCE (a.USD_NOTIONAL*a.COUPON,0) ::numeric(15,5)) as Notional_x_Coupon, SUM(COALESCE (USD_NOTIONAL,0) ::numeric(15,5)/1000) as USD_NOTIONAL, SUM(COALESCE (USD_MARKET_VALUE,0) ::numeric(15,5)/1000) as USD_MARKET_VALUE, SUM(COALESCE (USD_IR_UNIFIED_PV01,0) ::numeric(15,5)/1000) as USD_PV01, Sum(COALESCE (USD_PV01SPRD,0) ::numeric(15,5)/1000) as USD_PV01SPRD, SUM (COALESCE (USD_IR_KAPPA,0) ::numeric(15,5)/10000) as USD_IR_KAPPA, case when BOOK like '%JUMBO%' then 'FRM' when BOOK like '%YRARM%' then 'ARM' else '1mARM' end as Type from cdwuser.U_DM_WM A where COB_DATE in ( '2018-02-28', '2018-02-27', '2018-01-31', '2017-12-29', '2017-11-30', '2017-10-31', '2017-09-29', '2017-08-31', '2013-12-31' ) and CCC_HIERARCHY_LEVEL2 IN ('GLOBAL WEALTH MANAGEMENT', 'WEALTH MANAGEMENT') AND CCC_DIVISION IN ('PRIVATE BANKING GROUP', 'RETAIL BANKING GROUP', 'PWM US BRANCH') and VAR_EXCL_FL<> 'Y' AND CCC_TAPS_COMPANY = '6635' AND (ccc_business_area IN ('MSCC MORTGAGE LOAN ORIGINATIONS-GWMG') AND (BOOK LIKE ('%LOAN%') OR BOOK LIKE ('%PRODUCTION')) OR CCC_BUSINESS_AREA IN ('US BANKS-CRA PORTFOLIO')) Group by a.COB_DATE, a.CCC_TAPS_COMPANY, a.FACILITY_TYPE, a.coupon, a.VINTAGE_YEAR, a.VINTAGE, case when BOOK like '%JUMBO%' then 'FRM' when BOOK like '%YRARM%' then 'ARM' else '1mARM' end, case when COUPON <=5 then round(COUPON/.5,0)*.5 else round(COUPON,0) ::numeric(15,5) end, a.book