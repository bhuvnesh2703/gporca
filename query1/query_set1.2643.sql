with fid as ( SELECT case when currency_code in ('USD','EUR','GBP') then currency_code else 'OTHER' end as currency, case when currency_code in ('USD','EUR','GBP') then ccc_business_area else null end as ccc_business_area, term_new as term, SUM (CASE WHEN cob_date = '2018-02-28' THEN usd_ir_unified_pv01 ELSE 0 END) AS pv01, SUM (CASE WHEN cob_date = '2018-02-28' THEN usd_ir_unified_pv01 ELSE -usd_ir_unified_pv01 END) AS pv01_c FROM CDWUSER.U_EXP_TRENDS a where cob_date in ('2018-02-28','2018-02-27') AND var_excl_fl <> 'Y' AND PARENT_LEGAL_ENTITY = '1633' AND COALESCE(usd_ir_unified_pv01,0) <> 0 GROUP BY case when currency_code in ('USD','EUR','GBP') then currency_code else 'OTHER' end, case when currency_code in ('USD','EUR','GBP') then ccc_business_area else null end, term_new), src as (select currency, null as business_area, term, sum(pv01) as pv01, sum(pv01_c) as pv01_c from fid GROUP BY currency, term union all select currency, ccc_business_area as business_area, term, sum(pv01) as pv01, sum(pv01_c) as pv01_c from fid GROUP BY currency, ccc_business_area, term), ranked as (SELECT currency, business_area, sum(case when term = 0.083 then pv01 end) as pv01_0083, sum(case when term = 0.25 then pv01 end) as pv01_025, sum(case when term = 0.5 then pv01 end) as pv01_05, sum(case when term = 0.75 then pv01 end) as pv01_075, sum(case when term = 1 then pv01 end) as pv01_1, sum(case when term = 2 then pv01 end) as pv01_2, sum(case when term = 3 then pv01 end) as pv01_3, sum(case when term = 5 then pv01 end) as pv01_5, sum(case when term = 7 then pv01 end) as pv01_7, sum(case when term = 8 then pv01 end) as pv01_8, sum(case when term = 10 then pv01 end) as pv01_10, sum(case when term = 12 then pv01 end) as pv01_12, sum(case when term = 15 then pv01 end) as pv01_15, sum(case when term = 20 then pv01 end) as pv01_20, sum(case when term = 25 then pv01 end) as pv01_25, sum(case when term = 30 then pv01 end) as pv01_30, sum(case when term = 40 then pv01 end) as pv01_40, sum(case when term = 50 then pv01 end) as pv01_50, sum(case when term = 60 then pv01 end) as pv01_60, sum(case when term = 75 then pv01 end) as pv01_75, sum(pv01) as pv01, sum(case when term = 0.083 then pv01_c end) as pv01_0083_c, sum(case when term = 0.25 then pv01_c end) as pv01_025_c, sum(case when term = 0.5 then pv01_c end) as pv01_05_c, sum(case when term = 0.75 then pv01_c end) as pv01_075_c, sum(case when term = 1 then pv01_c end) as pv01_1_c, sum(case when term = 2 then pv01_c end) as pv01_2_c, sum(case when term = 3 then pv01_c end) as pv01_3_c, sum(case when term = 5 then pv01_c end) as pv01_5_c, sum(case when term = 7 then pv01_c end) as pv01_7_c, sum(case when term = 8 then pv01_c end) as pv01_8_c, sum(case when term = 10 then pv01_c end) as pv01_10_c, sum(case when term = 12 then pv01_c end) as pv01_12_c, sum(case when term = 15 then pv01_c end) as pv01_15_c, sum(case when term = 20 then pv01_c end) as pv01_20_c, sum(case when term = 25 then pv01_c end) as pv01_25_c, sum(case when term = 30 then pv01_c end) as pv01_30_c, sum(case when term = 40 then pv01_c end) as pv01_40_c, sum(case when term = 50 then pv01_c end) as pv01_50_c, sum(case when term = 60 then pv01_c end) as pv01_60_c, sum(case when term = 75 then pv01_c end) as pv01_75_c, sum(pv01_c) as pv01_c, case currency when 'USD' then 1 when 'EUR' then 2 when 'GBP' then 3 else 4 end as currency_order, case when business_area is null then 100 else least(rank() over(partition by currency order by abs(sum(coalesce(case when business_area is not null then pv01 else 0 end,0))) desc ) ,7) end as rank FROM src GROUP BY currency, business_area) SELECT currency_order, rank, currency, case rank when 7 then 'Other' else business_area end as business_area, sum(pv01_0083) pv01_0083, sum(pv01_0083_c) pv01_0083_c, sum(pv01_025) pv01_025, sum(pv01_025_c) pv01_025_c, sum(pv01_05) pv01_05, sum(pv01_05_c) pv01_05_c, sum(pv01_075) pv01_075, sum(pv01_075_c) pv01_075_c, sum(pv01_1) pv01_1, sum(pv01_1_c) pv01_1_c, sum(pv01_2) pv01_2, sum(pv01_2_c) pv01_2_c, sum(pv01_3) pv01_3, sum(pv01_3_c) pv01_3_c, sum(pv01_5) pv01_5, sum(pv01_5_c) pv01_5_c, sum(pv01_7) pv01_7, sum(pv01_7_c) pv01_7_c, sum(pv01_8) pv01_8, sum(pv01_8_c) pv01_8_c, sum(pv01_10) pv01_10, sum(pv01_10_c) pv01_10_c, sum(pv01_12) pv01_12, sum(pv01_12_c) pv01_12_c, sum(pv01_15) pv01_15, sum(pv01_15_c) pv01_15_c, sum(pv01_20) pv01_20, sum(pv01_20_c) pv01_20_c, sum(pv01_25) pv01_25, sum(pv01_25_c) pv01_25_c, sum(pv01_30) pv01_30, sum(pv01_30_c) pv01_30_c, sum(pv01_40) pv01_40, sum(pv01_40_c) pv01_40_c, sum(pv01_50) pv01_50, sum(pv01_50_c) pv01_50_c, sum(pv01_60) pv01_60, sum(pv01_60_c) pv01_60_c, sum(pv01_75) pv01_75, sum(pv01_75_c) pv01_75_c, sum(pv01) as pv01_total, sum(pv01_c) as pv01_total_c FROM ranked t GROUP BY currency_order, rank, currency, case rank when 7 then 'Other' else business_area end