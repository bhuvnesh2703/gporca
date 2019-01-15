SELECT CASE WHEN currency IN ('USD', 'EUR', 'GBP', 'JPY') THEN currency ELSE 'Other' END AS currency, usd_kappa_cob, usd_kappa_change FROM( SELECT currency_of_measure as currency, SUM(CASE WHEN cob_date = '2018-02-28' THEN coalesce(usd_ir_kappa, 0)/10 ELSE 0 END) AS usd_kappa_cob, SUM(CASE WHEN cob_date = '2018-02-28' THEN coalesce(usd_ir_kappa, 0)/10 ELSE -coalesce(usd_ir_kappa, 0)/10 END) AS usd_kappa_change FROM CDWUSER.U_EXP_MSR WHERE cob_date IN ('2018-02-28', '2018-01-31') AND PARENT_LEGAL_ENTITY = '0201(G)' AND VAR_EXCL_FL <> 'Y' GROUP BY currency_of_measure ) X ORDER BY abs(usd_kappa_cob) desc