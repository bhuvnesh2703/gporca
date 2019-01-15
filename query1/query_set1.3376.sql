select A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, sum(case when A.COB_DATE = '2018-02-28' then A.USD_PV10_BENCH else 0 end) as PV10_BENCH, sum(case when A.COB_DATE = '2018-02-21' then A.USD_PV10_BENCH else 0 end) as PV10_BENCH_COMP FROM cdwuser.U_exp_msr A WHERE A.COB_DATE IN ('2018-02-28','2018-02-21') AND A.CCC_PL_REPORTING_REGION = 'EMEA' AND A.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND A.CCC_PRODUCT_LINE not in ('DISTRESSED TRADING', 'PRIMARY - LOANS', 'PAR LOANS TRADING') AND A.FID1_SENIORITY NOT IN ('SUBT1', 'SUBUT2', 'AT1') AND A.MRD_RATING IN ('AAA', 'AA', 'A', 'BBB') and A.USD_PV10_BENCH IS NOT NULL group by A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by SUM (case when A.COB_DATE = '2018-02-28' then -A.USD_PV10_BENCH else 0 end) desc fetch first 10 rows only