SELECT p.cob_date as pnl_date, p.ccc_division as division, p.ccc_business_area as business_area, p.ccc_pl_reporting_region as pl_reporting_region, case when p.financial_element in ('AGENCY', 'MSCAP FEES', 'MSCI FEES', 'RISK') then 'Commissions' else 'Trading' end as Commissions, ROUND(SUM(p.daily_pl/1000000),3) as daily_pnl, ROUND (SUM (p.ytd_pl / 1000000),3) AS ytd_pnl FROM cdwuser.u_pct_pnl_current p WHERE p.ccc_division = 'INSTITUTIONAL EQUITY DIVISION' AND p.cob_date >= '2018-01-01' AND p.cob_date <= '2018-02-28' AND NOT p.COB_DATE = '2017-01-03' AND (p.account_purpose_code <> 'J' OR p.account_purpose_code IS NULL) GROUP BY p.cob_date, p.ccc_division, p.ccc_business_area, p.ccc_pl_reporting_region, case when p.financial_element in ('AGENCY', 'MSCAP FEES', 'MSCI FEES', 'RISK') then 'Commissions' else 'Trading' end UNION ALL SELECT p.cob_date as pnl_date, p.ccc_division as division, 'TOTAL ' || p.ccc_pl_reporting_region as BUSINESS_AREA, p.ccc_pl_reporting_region, case when p.financial_element in ('AGENCY', 'MSCAP FEES', 'MSCI FEES', 'RISK') then 'Commissions' else 'Trading' end as Commissions, ROUND(SUM(p.daily_pl/1000000),3) as daily_pnl, ROUND (SUM (p.ytd_pl / 1000000),3) AS ytd_pnl FROM cdwuser.u_pct_pnl_current p WHERE p.ccc_division = 'INSTITUTIONAL EQUITY DIVISION' AND p.cob_date >= '2018-01-01' AND p.cob_date <= '2018-02-28' AND NOT p.COB_DATE = '2017-01-03' AND (p.account_purpose_code <> 'J' OR p.account_purpose_code IS NULL) GROUP BY p.cob_date, p.ccc_division, p.ccc_pl_reporting_region, case when p.financial_element in ('AGENCY', 'MSCAP FEES', 'MSCI FEES', 'RISK') then 'Commissions' else 'Trading' end UNION SELECT p.cob_date as pnl_date, p.ccc_division as division, 'TOTAL ' || p.ccc_pl_reporting_region || ' excl. CVA, DVA, TCM' as BUSINESS_AREA, p.ccc_pl_reporting_region as pl_reporting_region, case when p.financial_element in ('AGENCY', 'MSCAP FEES', 'MSCI FEES', 'RISK') then 'Commissions' else 'Trading' end as Commissions, ROUND(SUM(p.daily_pl/1000000),3) as daily_pnl, ROUND (SUM (p.ytd_pl / 1000000),3) AS ytd_pnl FROM cdwuser.u_pct_pnl_current p WHERE p.ccc_division = 'INSTITUTIONAL EQUITY DIVISION' AND p.cob_date >= '2018-01-01' AND p.cob_date <= '2018-02-28' AND NOT p.COB_DATE = '2017-01-03' AND p.STRATEGY NOT IN ('MS CVA MPE - DERIVATIVES','MS CVA MNE - DERIVATIVES','MS DVA STR NOTES IED','TCM ALLOCATION IED','TCM ALLOCATION OFFSET','PB - TCM ALLOCATION','OTHER - TCM ALLOCATION','DERIVS - TCM ALLOCATION','CASH - TCM ALLOCATION','IWM - TCM ALLOCATION') AND (p.account_purpose_code <> 'J' OR p.account_purpose_code IS NULL) GROUP BY p.cob_date, p.ccc_pl_reporting_region, p.ccc_division, case when p.financial_element in ('AGENCY', 'MSCAP FEES', 'MSCI FEES', 'RISK') then 'Commissions' else 'Trading' end