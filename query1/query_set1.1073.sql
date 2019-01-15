SELECT     cob_date,     a.CURVE_NAME,     SUM (usd_delta) AS USD_DELTA FROM cdwuser.u_eq_msr a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-01') AND       a.book NOT IN ('CVASP') AND     (a.ccc_business_area IN ('CPM TRADING (MPE)', 'CPM', 'CREDIT', 'COMMODS FINANCING') OR      a.ccc_strategy IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) AND     a.usd_delta IS NOT NULL GROUP BY     cob_date,     a.CURVE_NAME