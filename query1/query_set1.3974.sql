SELECT
    A.COB_DATE,
    SUM (A.USD_PV10_BENCH) AS USD_PV10_BENCH
FROM cdwuser.U_DM_CC A
WHERE
    a.COB_DATE IN (
'2018-01-17', 
'2018-01-24', 
'2018-01-31', 
'2018-02-07', 
'2018-02-14', 
'2018-02-21', 
'2018-02-28'
) AND


    (A.CCC_STRATEGY LIKE '%PAR LOANS TRADING%' OR CCC_STRATEGY in ('PRIMARY LOANS - AP','SECONDARY LOANS - AP') OR CCC_PRODUCT_LINE = 'PAR LOANS TRADING' OR BOOK IN ('LDN PAR TRADING RG-LNRGO','LDN PAR TRADING SIDDIQUI-LNSSL')) AND
    A.CREDIT_TRADING_FLAG = 'Flow Trading'
GROUP BY
    A.COB_DATE