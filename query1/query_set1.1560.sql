WIth x as (SELECT a.ISSUE_ID_DECOMP, sum(coalesce(a.USD_EQ_DELTA_DECOMP,0))/1000 as USD_DELTA FROM CDWUSER.U_DECOMP_MSR a WHERE a.COB_DATE = '2018-02-28' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_TAPS_COMPANY = '0302' AND a.USD_EQ_DELTA_DECOMP <> 0 GROUP BY a.ISSUE_ID_DECOMP ORDER BY ABS(SUM(COALESCE(a.USD_EQ_DELTA_DECOMP,0))) DESC FETCH FIRST 10 ROWS ONLY ), y as ( SELECT DISTINCT a.ISSUE_ID_DECOMP, a.PRODUCT_DESCRIPTION_DECOMP FROM CDWUSER.U_DECOMP_MSR a WHERE a.COB_DATE = '2018-02-28' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_TAPS_COMPANY = '0302' AND a.USD_EQ_DELTA_DECOMP <> 0 ) SELECT RANK () OVER (ORDER BY ABS(x.USD_DELTA) DESC) AS RANK, x.ISSUE_ID_DECOMP, y.PRODUCT_DESCRIPTION_DECOMP, x.USD_DELTA FROM x LEFT JOIN y ON x.ISSUE_ID_DECOMP = y.ISSUE_ID_DECOMP ORDER BY ABS(x.USD_DELTA) DESC