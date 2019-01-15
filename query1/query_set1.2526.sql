SELECT * FROM( SELECT CASE WHEN a.pl_reporting_region = 'AMERICAS' THEN 1 ELSE 0 END AS IS_US, CASE WHEN UPPER(a.spg_desc) LIKE 'CMBS %' THEN 'Commercial Mortgage' WHEN UPPER(a.spg_desc) LIKE 'ABS %' THEN 'Aset Back' WHEN UPPER(a.spg_desc) LIKE 'RMBS %' THEN 'RMBS' WHEN UPPER(a.spg_desc) LIKE 'CORPORATE %' THEN 'Corporate' WHEN UPPER(a.spg_desc) LIKE 'WAREHOUSE %' THEN 'Warehouse' WHEN UPPER(a.spg_desc) LIKE 'COLONNADE %' THEN 'Colonnade' ELSE a.spg_desc END AS LEVEL1, CASE WHEN (UPPER(a.spg_desc) LIKE 'RMBS AGENCY %') OR (UPPER(a.spg_desc) LIKE 'RMBS MBS %') THEN 'Agency Total' WHEN (UPPER(a.spg_desc) LIKE 'RMBS IOS %') OR (UPPER(a.spg_desc) LIKE 'RMBS MBX %') OR (UPPER(a.spg_desc) LIKE 'RMBS PO %') OR (UPPER(a.spg_desc) LIKE 'RMBS PRIME %') THEN 'Prime Total' WHEN (UPPER(a.spg_desc) LIKE 'RMBS OPTION %') THEN 'Option Total' WHEN (UPPER(a.spg_desc) LIKE 'RMBS ALTA %') THEN 'Alta Total' WHEN (UPPER(a.spg_desc) LIKE 'RMBS SECOND %') THEN 'Second Total' WHEN (UPPER(a.spg_desc) LIKE 'RMBS CDO%') OR (UPPER(a.spg_desc) LIKE 'RMBS SUB PRIME %') OR (UPPER(a.spg_desc) LIKE 'RMBS SUPER SENIOR%') OR (UPPER(a.spg_desc) IN ('RMBS ABSPOKE', 'RMBS DEFAULT SWAP', 'RMBS INDEX TRANCHE', 'RMBS NIMS', 'RMBS NON CONFORSUMG DEFAULT SWAP', 'RMBS POST NIM', 'RMBS NON CONFORMING DEFAULT SWAP')) THEN 'Sub Prime Total' WHEN (UPPER(a.spg_desc) LIKE 'RMBS HELOC %') THEN 'HELOC Total' WHEN (UPPER(a.spg_desc) LIKE 'RMBS SD %') THEN 'S&D Total' WHEN (UPPER(a.spg_desc) LIKE 'RMBS%') THEN 'Other' ELSE a.spg_desc END AS LEVEL2, CASE WHEN a.rating IN ('CCC', 'CC', 'C', 'D') THEN '<B' ELSE a.rating END AS rating2, CASE WHEN a.rating IN ('AAA', 'AA', 'A', 'BBB') THEN 'Inv Gr' WHEN a.rating IN ('BB', 'B', 'CCC', 'CC', 'C', 'D') THEN 'HY' ELSE a.rating END AS grade, a.spg_desc, sum(case when vardate= '2018-02-28' and UPPER(a.spg_desc) not in ('SWAP', 'RATE FUTURES', 'REPO MV') then coalesce(a.usd_net_exposure,0) else 0 end) as usd_net_exposure_cob, sum(case when vardate= '2018-02-28' and UPPER(a.spg_desc) not in ('SWAP', 'RATE FUTURES', 'REPO MV') then coalesce(a.usd_net_exposure,0) else case when UPPER(a.spg_desc) not in ('SWAP', 'RATE FUTURES', 'REPO MV') then -coalesce(a.usd_net_exposure,0) else 0 end end) as usd_net_exposure_change, sum(case when vardate= '2018-02-28' and UPPER(a.spg_desc) not in ('SWAP', 'RATE FUTURES', 'REPO MV') then coalesce(a.usd_proceed,0) else 0 end) as usd_proceed_cob, sum(case when vardate= '2018-02-28' and UPPER(a.spg_desc) not in ('SWAP', 'RATE FUTURES', 'REPO MV') then coalesce(a.usd_proceed,0) else case when UPPER(a.spg_desc) not in ('SWAP', 'RATE FUTURES', 'REPO MV') then -coalesce(a.usd_proceed,0) else 0 end end) as usd_proceed_change, sum(case when vardate= '2018-02-28' then coalesce(a.usd_kappa,0) else 0 end) as usd_kappa_cob, sum(case when vardate= '2018-02-28' then coalesce(a.usd_kappa,0) else -coalesce(a.usd_kappa,0) end) as usd_kappa_change, sum(case when vardate= '2018-02-28' then coalesce(a.usd_pv01,0) else 0 end) as usd_pv01_cob, sum(case when vardate= '2018-02-28' then coalesce(a.usd_pv01,0) else -coalesce(a.usd_pv01,0) end) as usd_pv01_change, sum(case when vardate= '2018-02-28' then coalesce(a.usd_pv01sprd_ba,0) else 0 end) as usd_pv01sprd_ba_cob, sum(case when vardate= '2018-02-28' then coalesce(a.usd_pv01sprd_ba,0) else -coalesce(a.usd_pv01sprd_ba,0) end) as usd_pv01sprd_ba_change FROM ( select cob_date as vardate, vertical_system, ccc_pl_reporting_region as pl_reporting_region, ccc_business_area, PARENT_LEGAL_ENTITY, CCC_TAPS_COMPANY, spg_desc, mrd_rating as rating, var_excl_fl, ccc_banking_trading, usd_exposure as usd_net_exposure, usd_market_value as usd_proceed, usd_ir_kappa as usd_kappa, coalesce(usd_ir_unified_pv01, 0) as usd_pv01, usd_pv01sprd as usd_pv01sprd_ba from CDWUSER.u_exp_MSR ) a WHERE (a.vardate IN ('2018-02-28', '2018-02-27')) AND (a.VERTICAL_SYSTEM LIKE ('SPG%') OR ccc_business_area IN ('SECURITIZED PRODUCTS GRP')) AND PARENT_LEGAL_ENTITY = '0517(G)' and (a.var_excl_fl <> 'Y') AND ccc_banking_trading = 'TRADING' GROUP BY IS_US, LEVEL1, LEVEL2, rating2, grade, a.spg_desc ) X WHERE abs(coalesce(usd_net_exposure_cob, 0)) + abs(coalesce(usd_net_exposure_change, 0)) + abs(coalesce(usd_proceed_cob, 0)) + abs(coalesce(usd_proceed_change, 0)) + abs(coalesce(usd_kappa_cob, 0)) + abs(coalesce(usd_kappa_change, 0)) + abs(coalesce(usd_pv01_cob, 0)) + abs(coalesce(usd_pv01_change, 0)) + abs(coalesce(usd_pv01sprd_ba_cob, 0)) + abs(coalesce(usd_pv01sprd_ba_change, 0)) <> 0