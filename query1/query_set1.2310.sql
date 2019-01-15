with fid as ( SELECT case when product_type_code in ('BONDFUT', 'BONDOPT', 'GVTBONDIL', 'GVTBOND', 'BONDFUTOPT') then 'Total Govts' when upper(sectype2) = upper('Govts') then 'Total Govts' when upper(curve_name) = upper('krwktb') then 'Total Govts' when upper(sectype2) = upper('Swaps') then 'Total Swaps' when product_type_code in ('RATEFUT', 'SWAPS') then 'Total Swaps' else 'Other' end as sectype_grouping, case product_type_code when 'BONDFUT' then 'Bond Futures' when 'GVTBOND' then 'Govt Bonds' when 'GVTBONDIL' then 'Govt Bonds' when 'RATEFUT' then 'Rate Future' else case when sectype2='OTHER' then product_type_code else sectype2 end end as sectype, coalesce(currency_of_measure,'OTH') currency, SUM (CASE WHEN cob_date = '2018-02-28' THEN usd_convx ELSE 0 END) AS usd_gamma, SUM (CASE WHEN cob_date = '2018-02-28' THEN usd_convx ELSE -usd_convx END) AS usd_gamma_c FROM CDWUSER.U_IR_MSR where cob_date in ('2018-02-28','2018-02-27') AND var_excl_fl <> 'Y' AND PARENT_LEGAL_ENTITY = '0302(G)' AND COALESCE(usd_convx,0) <> 0 AND ccc_banking_trading = 'TRADING' GROUP BY case when product_type_code in ('BONDFUT', 'BONDOPT', 'GVTBONDIL', 'GVTBOND', 'BONDFUTOPT') then 'Total Govts' when upper(sectype2) = upper('Govts') then 'Total Govts' when upper(curve_name) = upper('krwktb') then 'Total Govts' when upper(sectype2) = upper('Swaps') then 'Total Swaps' when product_type_code in ('RATEFUT', 'SWAPS') then 'Total Swaps' else 'Other' end, case product_type_code when 'BONDFUT' then 'Bond Futures' when 'GVTBOND' then 'Govt Bonds' when 'GVTBONDIL' then 'Govt Bonds' when 'RATEFUT' then 'Rate Future' else case when sectype2='OTHER' then product_type_code else sectype2 end end, coalesce(currency_of_measure,'OTH') ), src as (select sectype_grouping, null as sectype, currency, sum(usd_gamma) as usd_gamma, sum(usd_gamma_c) as usd_gamma_c from fid GROUP BY sectype_grouping, currency union all select sectype_grouping, sectype, currency, sum(usd_gamma) as usd_gamma, sum(usd_gamma_c) as usd_gamma_c from fid GROUP BY sectype_grouping, sectype, currency), ranked as (SELECT sectype_grouping, sectype, sum(case when currency = 'USD' then usd_gamma end) as usd_gamma_USD, sum(case when currency = 'GBP' then usd_gamma end) as usd_gamma_GBP, sum(case when currency = 'EUR' then usd_gamma end) as usd_gamma_EUR, sum(case when currency = 'JPY' then usd_gamma end) as usd_gamma_JPY, sum(case when currency = 'AUD' then usd_gamma end) as usd_gamma_AUD, sum(case when currency = 'CAD' then usd_gamma end) as usd_gamma_CAD, sum(case when currency = 'CHF' then usd_gamma end) as usd_gamma_CHF, sum(case when currency not in ('USD','GBP','EUR','JPY','AUD','CAD','CHF') then usd_gamma end) as usd_gamma_oth, sum(usd_gamma) as usd_gamma, sum(case when currency = 'USD' then usd_gamma_c end) as usd_gamma_USD_c, sum(case when currency = 'GBP' then usd_gamma_c end) as usd_gamma_GBP_c, sum(case when currency = 'EUR' then usd_gamma_c end) as usd_gamma_EUR_c, sum(case when currency = 'JPY' then usd_gamma_c end) as usd_gamma_JPY_c, sum(case when currency = 'AUD' then usd_gamma_c end) as usd_gamma_AUD_c, sum(case when currency = 'CAD' then usd_gamma_c end) as usd_gamma_CAD_c, sum(case when currency = 'CHF' then usd_gamma_c end) as usd_gamma_CHF_c, sum(case when currency not in ('USD','GBP','EUR','JPY','AUD','CAD','CHF') then usd_gamma_c end) as usd_gamma_oth_c, sum(usd_gamma_c) as usd_gamma_c, case sectype_grouping when 'Total Govts' then 1 when 'Total Swaps' then 2 else 3 end as sectype_grouping_order, case when sectype is null then 0 else least(rank() over(partition by sectype_grouping order by abs(sum(coalesce(case when sectype is not null then usd_gamma else 0 end,0))) desc ) , case sectype_grouping when 'Total Govts' then 4 when 'Total Swaps' then 2 else 9 end) end as rank FROM src GROUP BY sectype_grouping, sectype) SELECT sectype_grouping_order, rank, sectype_grouping, case rank when case sectype_grouping when 'Total Govts' then 4 when 'Total Swaps' then 2 else 9 end then 'Other' else sectype end as sectype, sum(usd_gamma_USD) usd_gamma_USD, sum(usd_gamma_USD_c) usd_gamma_USD_c, sum(usd_gamma_GBP) usd_gamma_GBP, sum(usd_gamma_GBP_c) usd_gamma_GBP_c, sum(usd_gamma_EUR) usd_gamma_EUR, sum(usd_gamma_EUR_c) usd_gamma_EUR_c, sum(usd_gamma_JPY) usd_gamma_JPY, sum(usd_gamma_JPY_c) usd_gamma_JPY_c, sum(usd_gamma_AUD) usd_gamma_AUD, sum(usd_gamma_AUD_c) usd_gamma_AUD_c, sum(usd_gamma_CAD) usd_gamma_CAD, sum(usd_gamma_CAD_c) usd_gamma_CAD_c, sum(usd_gamma_CHF) usd_gamma_CHF, sum(usd_gamma_CHF_c) usd_gamma_CHF_c, sum(usd_gamma_oth) usd_gamma_oth, sum(usd_gamma_oth_c) usd_gamma_oth_c, sum(usd_gamma) usd_gamma, sum(usd_gamma_c) usd_gamma_c FROM ranked t GROUP BY sectype_grouping_order, rank, sectype_grouping, case rank when case sectype_grouping when 'Total Govts' then 4 when 'Total Swaps' then 2 else 9 end then 'Other' else sectype end