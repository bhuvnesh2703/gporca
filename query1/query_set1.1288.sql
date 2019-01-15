SELECT a.COB_DATE, sum(a.SLIDE_EQ_MIN_05_USD) + 0.05 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_MIN_05_USD, sum(a.SLIDE_EQ_MIN_10_USD) + 0.1 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_MIN_10_USD, (sum(a.SLIDE_EQ_MIN_10_USD) + sum(a.SLIDE_EQ_MIN_20_USD))/2 + 0.15 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_MIN_15_USD, sum(a.SLIDE_EQ_MIN_20_USD) + 0.2 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_MIN_20_USD, (sum(a.SLIDE_EQ_MIN_20_USD) + sum(a.SLIDE_EQ_MIN_30_USD))/2 + 0.25 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_MIN_25_USD, sum(a.SLIDE_EQ_MIN_30_USD) + 0.3 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_MIN_30_USD, sum(a.SLIDE_EQ_PLS_05_USD) - 0.05 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_PLS_05_USD, sum(a.SLIDE_EQ_PLS_10_USD) - 0.10 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_PLS_10_USD, (sum(a.SLIDE_EQ_PLS_10_USD) + sum(a.SLIDE_EQ_PLS_20_USD))/2 - 0.15 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_PLS_15_USD, sum(a.SLIDE_EQ_PLS_20_USD) - 0.2 * sum(a.USD_EQ_DELTA_DECOMP) as SLIDE_EQ_PLS_20_USD FROM CDWUSER.U_DM_EQ a WHERE a.COB_DATE in ('2018-02-28','2018-01-31') AND a.CCC_BANKING_TRADING = 'TRADING' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' GROUP BY a.COB_DATE