Select Cob_Date, case when CREDIT_SPREAD < 50 then '<50' when CREDIT_SPREAD >= 50 and CREDIT_SPREAD < 100 then '50-100' when CREDIT_SPREAD >= 100 and CREDIT_SPREAD < 200 then '100-200' when CREDIT_SPREAD >=200 and CREDIT_SPREAD < 500 then '200-500' when CREDIT_SPREAD >= 500 and CREDIT_SPREAD < 1000 then '500-1000' when CREDIT_SPREAD >= 1000 then '>1000' else 'Others' end as SPREAD_BUCKET, Net_Exposure, CR_PV01, CR_PV10 from (select c.COB_DATE, c.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME as Issuer, max(c.MRD_RATING) as Rating, sum(case when SILO_SRC = 'IED' then c.MEASURE_MARK else CREDIT_SPREAD end) as Credit_Spread, sum(coalesce(c.USD_EXPOSURE,0)) as Net_Exposure, sum(coalesce(c.usd_pv01sprd,0)) as CR_PV01, sum(case when SILO_SRC = 'IED' then c.USD_CREDIT_PV10PCT else c.USD_PV10_BENCH end) as CR_PV10 from cdwuser.U_CR_MSR c where c.COB_DATE in ('2018-02-28', '2018-02-27') and c.DIVISION = 'IED' and c.CCC_BANKING_TRADING = 'TRADING' and c.CCC_PL_REPORTING_REGION = 'EMEA' and c.CCC_PRODUCT_LINE = 'CONVERTIBLE PRODUCTS' and c.PRODUCT_TYPE_CODE in ('BOND','ASCOT','CONVRT') group by c.COB_DATE, c.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, c.POSITION_ID) a