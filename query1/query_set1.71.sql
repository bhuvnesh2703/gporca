select      COB_DATE,     PRODUCT_TYPE_CODE,     case         when CR_ULTIMATE_CNTRY_CODE in ('MAC','PAK') then 'Other Asia'     else CR_ULTIMATE_CNTRY_CODE end as COUNTRY_CODE,     case         when a.FID1_INDUSTRY_NAME_LEVEL1 in ('SOVEREIGN', 'GOVERNMENT SPONSORED') then 'SOVEREIGN'     ELSE 'NON-SOVEREIGN' END AS IS_SOVEREIGN,     case         when MRD_RATING in ('AAA','AA','A','BBB') then 'IG'     else 'NIG' end as IG_NIG,     case         when TERM_BUCKET in ('0-0.083Y', '0.083-0.25Y','0.25-0.5Y', '0.5-0.75Y', '0.75-1Y') then '0-1YR'          when TERM_BUCKET in ('1-2Y', '2-3Y', '3-5Y') THEN '1-5YR'         when TERM_BUCKET in ('5-7Y', '7-8Y', '8-10Y') then '5-10YR'         when TERM_BUCKET in ('10-12Y','12-15Y') then '10-15YR'      else  '15+YR' end as GROUPED_TERM_BUCKET,      sum(USD_PV01SPRD) as SPV01,     sum(USD_PV10_BENCH) as PV10,     sum(USD_EXPOSURE) as NET_EXPOSURE from     cdwuser.U_EXP_MSR a where (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-21') and A.CCC_PL_REPORTING_REGION in ('ASIA PACIFIC') AND       a.CCC_DIVISION = 'FIXED INCOME DIVISION' AND     (a.CCC_BUSINESS_AREA NOT IN ('LENDING','FID MANAGEMENT') AND a.CCC_PRODUCT_LINE <> 'DISTRESSED TRADING') AND     /*a.CCC_BUSINESS_AREA NOT IN ('COMMODITIES') AND*/      a.CCC_BANKING_TRADING = 'TRADING' group by     COB_DATE,     PRODUCT_TYPE_CODE,     case         when CR_ULTIMATE_CNTRY_CODE in ('MAC','PAK') then 'Other Asia'     else CR_ULTIMATE_CNTRY_CODE end,      case         when a.FID1_INDUSTRY_NAME_LEVEL1 in ('SOVEREIGN', 'GOVERNMENT SPONSORED') then 'SOVEREIGN'     ELSE 'NON-SOVEREIGN' END,     case         when MRD_RATING in ('AAA','AA','A','BBB') then 'IG'     else 'NIG' end,     case         when TERM_BUCKET in ('0-0.083Y', '0.083-0.25Y','0.25-0.5Y', '0.5-0.75Y', '0.75-1Y') then '0-1YR'          when TERM_BUCKET in ('1-2Y', '2-3Y', '3-5Y') THEN '1-5YR'         when TERM_BUCKET in ('5-7Y', '7-8Y', '8-10Y') then '5-10YR'         when TERM_BUCKET in ('10-12Y','12-15Y') then '10-15YR'      else  '15+YR' end