SELECT v.COB_DATE, v.IS_PEGGED, v.IS_UK_GROUP, v.CCC_PL_REPORTING_REGION,v.CURRENCY_OF_MEASURE, SUM (USD_FX) AS USD_FX FROM CDWUSER.U_DM_FX v WHERE v.COB_DATE in ('2018-02-28','2018-02-21','2018-02-14','2018-02-07','2018-02-07','2018-01-31','2018-01-24','2018-01-17','2018-01-10','2018-01-03') AND v.CURRENCY_OF_MEASURE in ('AED','BGN','CNH','CNY','CZK','DKK','HKD','ILS','KWD','KZT','OMR','QAR','RUB','SAR','SGD','TWD','CHF') GROUP BY v.COB_DATE, v.IS_PEGGED, v.IS_UK_GROUP, v.CCC_PL_REPORTING_REGION,v.CURRENCY_OF_MEASURE FOR READ ONLY