SELECT t.COB_DATE, t.BOOK, t. FACILITY_TYPE, t.RESET_DATE, EXTRACT (year FROM t.RESET_DATE)|| EXTRACT (quarter FROM t.RESET_DATE)||'Q' as RANK_DATE, EXTRACT (quarter FROM t.RESET_DATE)||'Q '|| EXTRACT (year FROM t.RESET_DATE) as RESET_Q, SUM(t.NOTIONAL) ::numeric(15,5) as TOTAL_NOTIONAL FROM ( SELECT A.COB_DATE, (a.COB_DATE + interval '6 months') as end_date, a.book, a.FACILITY_TYPE, CASE WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('1M') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('1M')) THEN (A.ISSUE_DATE + interval '1 MONTHS') WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('3Y') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('3Y')) THEN (A.ISSUE_DATE + interval '36 MONTHS') WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('5Y') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('5Y')) THEN (A.ISSUE_DATE + interval '60 MONTHS') WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('7Y') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('7Y')) THEN (A.ISSUE_DATE + interval '84 MONTHS') WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('10') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('10')) THEN (A.ISSUE_DATE + interval '120 MONTHS') ELSE A.ISSUE_DATE END AS RESET_DATE, SUM(A.USD_NOTIONAL) ::numeric(15,5) AS NOTIONAL, SUM(A.USD_MARKET_VALUE) ::numeric(15,5) AS MARKET_VALUE, SUM(A.USD_IR_UNIFIED_PV01) ::numeric(15,5) AS PV01 FROM cdwuser.U_DM_WM A WHERE A.COB_DATE in ( '2018-02-21', '2018-01-31', '2017-12-29', '2017-11-30', '2017-10-31', '2017-09-29', '2017-08-31', '2017-07-31', '2017-06-30', '2017-12-31', '2017-09-30', '2017-06-30', '2017-03-31', '2016-12-30', '2016-09-30' ) AND A.CCC_TAPS_COMPANY = '6635' AND A.VAR_EXCL_FL<> 'Y' AND (A.CCC_STRATEGY = 'HELD FOR INVESTMENT - (HFI)' or A.CCC_DIVISION = 'HELD FOR INVESTMENT - (HFI)' or A.CCC_PRODUCT_LINE ='HELD FOR INVESTMENT - (HFI)') AND A.BOOK LIKE '%ARM%' group by A.COB_DATE, a.book, a.FACILITY_TYPE, CASE WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('1M') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('1M')) THEN (A.ISSUE_DATE + interval '1 MONTHS') WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('3Y') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('3Y')) THEN (A.ISSUE_DATE + interval '36 MONTHS') WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('5Y') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('5Y')) THEN (A.ISSUE_DATE + interval '60 MONTHS') WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('7Y') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('7Y')) THEN (A.ISSUE_DATE + interval '84 MONTHS') WHEN (SUBSTR(A.BOOK, posstr(A.BOOK, 'HTM') + 4, 2) IN ('10') or SUBSTR(A.BOOK, posstr(A.BOOK, 'HFI') + 4, 2) IN ('10')) THEN (A.ISSUE_DATE + interval '120 MONTHS') ELSE A.ISSUE_DATE END ) t WHERE t.RESET_DATE between t.COB_DATE and t.end_date GROUP BY t.COB_DATE, t.BOOK, t.FACILITY_TYPE, t.RESET_DATE, EXTRACT (year FROM t.RESET_DATE)|| EXTRACT (quarter FROM t.RESET_DATE)||'Q' , EXTRACT (quarter FROM t.RESET_DATE)||'Q '|| EXTRACT (year FROM t.RESET_DATE)