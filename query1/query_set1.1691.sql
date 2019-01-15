select a.COB_DATE, case when Substr(CURVE_CURRENCY_PAIR,1,3) in ('N/A','USD','AUD','CAD','CHF','DKK','EUR','GBP','JPY','NOK','NZD','SEK','UBD', 'UB1') and (Substr(CURVE_CURRENCY_PAIR,4,3) in ('N/A','USD','AUD','CAD','CHF','DKK','EUR','GBP','JPY','NOK','NZD','SEK','UBD', 'UB1') or Substr(CURVE_CURRENCY_PAIR,4,3)='') then 'MAJOR Vs MAJOR' when ((Substr(CURVE_CURRENCY_PAIR,1,3) in ('HKD','SAR','AED','QAR','KWD','BHD')) and (Substr(CURVE_CURRENCY_PAIR,4,3) in ('N/A','USD')) or (Substr(CURVE_CURRENCY_PAIR,1,3) in ('N/A','USD')) and (Substr(CURVE_CURRENCY_PAIR,4,3) in ('HKD','SAR','AED','QAR','KWD','BHD'))) then 'USD Vs PEGGED' when ((Substr(CURVE_CURRENCY_PAIR,1,3) in ('CNH','CNY','CNX')) and (Substr(CURVE_CURRENCY_PAIR,4,3) in ('N/A','USD')) or (Substr(CURVE_CURRENCY_PAIR,1,3) in ('N/A','USD')) and (Substr(CURVE_CURRENCY_PAIR,4,3) in ('CNH','CNY','CNX'))) then 'USD Vs CHINA' else 'MAJOR-EM Vs EM' end as CURRENCY_GROUP, case when CCC_TAPS_COMPANY = '0302' then 'Y' else 'N' end as MSIP, case when CCC_PL_REPORTING_REGION = 'EMEA' then 'Y' else 'N' end as EMEA, sum(usd_fx_kappa) as usd_fx_kappa from cdwuser.U_DM_FX a where COB_DATE >= '2017-01-01' AND ccc_business_area IN ('FXEM MACRO TRADING','EM CREDIT TRADING') and a.usd_fx_kappa <> 0 group by a.COB_DATE, a.CURVE_CURRENCY_PAIR, case when CCC_TAPS_COMPANY = '0302' then 'Y' else 'N' end, case when CCC_PL_REPORTING_REGION = 'EMEA' then 'Y' else 'N' end