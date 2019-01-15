select a.COB_DATE, a.BOOK, a.PRODUCT_DESCRIPTION, a.PRODUCT_TYPE_CODE, case when product_type_code='AGNCMO' then 'Agency CMO' else a.PRODUCT_DESCRIPTION_MAPPING end as PRODUCT_DESCRIPTION_MAPPING, a.EXPIRATION_DATE, case when product_type_code='GVTBOND' AND PRODUCT_DESCRIPTION like '%US TREASURY NOTE%' and PRODUCT_DESCRIPTION like '%FLOAT%' THEN 'Treasury Float' when a.PRODUCT_DESCRIPTION_MAPPING in ('FMCC 15 Years', 'FMCC 30 Years', 'FNMA 15 Years', 'FNMA 30 Years') then 'FMCC/FNMA Pass-Throughs' when a.PRODUCT_DESCRIPTION_MAPPING in ('GNMA 15 Years', 'GNMA 30 Years') then 'GNMA Pass-Throughs' when product_type_code='AGNCMO' then 'Agency CMO' else a.PRODUCT_DESCRIPTION_MAPPING end as Product_Description_Charts, case when 12*a.TERM_OF_MEASURE/365 <='36' then '0-3 Years' when 12*a.TERM_OF_MEASURE/365 <='60' then '3-5 Years' when 12*a.TERM_OF_MEASURE/365 <='84' then '5-7 Years' when 12*a.TERM_OF_MEASURE/365 <='120' then '7-10 Years' else '10+ Years' end as Term, case when a.CURRENCY_OF_MEASURE not in ('USD','EUR','GBP', 'JPY') THEN 'OTHER' ELSE A.CURRENCY_OF_MEASURE END AS CURRENCY_OF_MEASURE, sum(coalesce(CASE WHEN book in('SWPIN','TDET6', 'TAIRR', 'TAEMR', 'TEEMR', 'HKFAP', 'TTIRR', 'HKFAM', 'TKFVS', 'HKFAS', 'TBAGR') THEN -usd_notional WHEN (book in('CALLT', 'CALTB', 'LCALT', 'LCATB')) THEN usd_MARKET_VALUE else usd_notional END, 0)) as USD_NOTIONAL, SUM(USD_EXPOSURE) AS USD_EXPOSURE, SUM(USD_PV01SPRD) AS USD_PV01SPRD, SUM(USD_IR_UNIFIED_PV01) AS USD_IR_UNIFIED_PV01, sum(a.USD_MARKET_VALUE) as Market_Value, sum(SLIDE_IR_PLS_200BP_USD)/1000 as SLIDE_IR_PLS_200BP_USD from cdwuser.U_DM_TREASURY a where CCC_DIVISION='TREASURY CAPITAL MARKETS' AND BOOK in ('TSTPM', 'TSHTM') and COB_DATE IN ('2018-02-28', '2018-02-27', '2018-02-27', '2018-02-26', '2018-02-23', '2018-02-22', '2018-02-27', '2018-01-31', '2017-12-29', '2017-11-30') group by a.COB_DATE, a.BOOK, a.PRODUCT_DESCRIPTION, a.PRODUCT_TYPE_CODE, case when product_type_code='AGNCMO' then 'Agency CMO' else a.PRODUCT_DESCRIPTION_MAPPING end, a.EXPIRATION_DATE, case when product_type_code='GVTBOND' AND PRODUCT_DESCRIPTION like '%US TREASURY NOTE%' and PRODUCT_DESCRIPTION like '%FLOAT%' THEN 'Treasury Float' when a.PRODUCT_DESCRIPTION_MAPPING in ('FMCC 15 Years', 'FMCC 30 Years', 'FNMA 15 Years', 'FNMA 30 Years') then 'FMCC/FNMA Pass-Throughs' when a.PRODUCT_DESCRIPTION_MAPPING in ('GNMA 15 Years', 'GNMA 30 Years') then 'GNMA Pass-Throughs' when product_type_code='AGNCMO' then 'Agency CMO' else a.PRODUCT_DESCRIPTION_MAPPING end, case when 12*a.TERM_OF_MEASURE/365 <='36' then '0-3 Years' when 12*a.TERM_OF_MEASURE/365 <='60' then '3-5 Years' when 12*a.TERM_OF_MEASURE/365 <='84' then '5-7 Years' when 12*a.TERM_OF_MEASURE/365 <='120' then '7-10 Years' else '10+ Years' end, case when a.CURRENCY_OF_MEASURE not in ('USD','EUR','GBP', 'JPY') THEN 'OTHER' ELSE A.CURRENCY_OF_MEASURE END