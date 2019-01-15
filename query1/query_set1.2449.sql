WITH SRC AS ( SELECT CASE WHEN CURRENCY = 'USD' THEN 1 WHEN CURRENCY = 'EUR' THEN 2 WHEN CURRENCY = 'GBP' THEN 3 WHEN CURRENCY = 'JPY' THEN 4 ELSE 5 END AS CURR_RANK, CURRENCY, TERM_NEW2, SUM(USD_PV01_COB) OVER (PARTITION BY CURRENCY) AS PV01_COB_RANK, USD_PV01_COB, USD_PV01_CHANGE, USD_KAPPA_COB, USD_KAPPA_CHANGE FROM ( SELECT CASE WHEN currency_code = 'CNY' AND ONSHORE_FL = 'N' THEN 'CNH' WHEN currency_code = 'KRW' AND ONSHORE_FL = 'N' THEN 'KRX' WHEN currency_code = 'BRL' AND ONSHORE_FL = 'N' THEN 'BRX' ELSE currency_code END AS CURRENCY, CASE WHEN TERM_NEW > 0 AND TERM_NEW <=2 THEN '0-2 YR' WHEN TERM_NEW > 2 AND TERM_NEW <=10 THEN '2-10 YR' ELSE '>10 YR' END AS TERM_NEW2, SUM (case when cob_date = '2018-02-28' then coalesce(usd_ir_unified_pv01, 0) else 0 end) AS usd_pv01_cob, SUM (case when cob_date = '2018-02-28' then coalesce(usd_ir_unified_pv01, 0) else -coalesce(usd_ir_unified_pv01, 0) end) AS usd_pv01_change, SUM (case when cob_date = '2018-02-28' then coalesce(usd_ir_kappa, 0) else 0 end) / 10 AS usd_kappa_cob, SUM (case when cob_date = '2018-02-28' then coalesce(usd_ir_kappa, 0) else -coalesce(usd_ir_kappa, 0) end) / 10 AS usd_kappa_change FROM CDWUSER.U_EXP_TRENDS WHERE cob_date IN ('2018-02-28', '2018-02-21') AND PARENT_LEGAL_ENTITY = '0302(G)' AND VAR_EXCL_FL <> 'Y' AND CCC_BANKING_TRADING = 'TRADING' GROUP BY CURRENCY, TERM_NEW2 ) X ) select case when rank >= 12 then 12 else rank end as rank, case when rank >= 12 then 'Other' else currency end as currency, term_new2, sum(usd_pv01_cob) as usd_pv01_cob, sum(usd_pv01_change) as usd_pv01_change, sum(usd_kappa_cob) as usd_kappa_cob, sum(usd_kappa_change) as usd_kappa_change from ( select dense_rank() over(order by CASE WHEN currency = 'USD' THEN 1 WHEN currency = 'EUR' THEN 2 WHEN currency = 'GBP' THEN 3 WHEN currency = 'JPY' THEN 4 ELSE 5 END, SUM desc) as rank, X.* FROM ( SELECT src.*, abs( sum(src.usd_pv01_cob) over(partition by src.currency) ) as SUM FROM SRC ) X ) Y group by case when rank >= 12 then 12 else rank end, case when rank >= 12 then 'Other' else currency end, term_new2