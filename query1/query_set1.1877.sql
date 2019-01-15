SELECT COB_DATE, sum(DELTA) as DELTA, sum(Abs_Delta) as GROSS_DELTA from (SELECT d.COB_DATE, d.ISSUE_ID_DECOMP, sum(coalesce(CASE WHEN coalesce(d.CASH_ISSUE_TYPE,'') <>'COMM' THEN d.USD_EQ_DELTA_DECOMP END,0)) as Delta, abs(sum(coalesce(CASE WHEN coalesce(d.CASH_ISSUE_TYPE,'') <>'COMM' THEN d.USD_EQ_DELTA_DECOMP END,0))) as Abs_Delta FROM CDWUSER.U_DECOMP_MSR d WHERE d.COB_DATE in ('2018-02-28','2018-02-21','2018-02-14','2018-02-07','2018-02-07','2018-01-31','2018-01-24','2018-01-17','2018-01-10','2018-01-03') AND d.CCC_BANKING_TRADING = 'TRADING' AND d.SILO_SRC = 'IED' AND d.DIVISION = 'IED' AND d.LE_GROUP = 'UK' group by d.COB_DATE, d.ISSUE_ID_DECOMP) x Group By COB_DATE FOR READ ONLY