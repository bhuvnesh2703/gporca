SELECT
    A.COB_DATE,
    SUM (A.USD_PV10_BENCH) AS USD_PV10_BENCH
FROM cdwuser.U_DM_CC A
WHERE
    a.COB_DATE IN (
'2017-12-27', 
'2018-01-03', 
'2018-01-10', 
'2018-01-17', 
'2018-01-24', 
'2018-01-31', 
'2018-02-28'
) AND
    A.CREDIT_CORPS_CCC_BUSINESS_AREA IN ('CREDIT-CORPORATES') AND


    A.CCC_STRATEGY NOT LIKE '%PAR LOANS TRADING%' AND CCC_STRATEGY NOT in ('PRIMARY LOANS - AP','SECONDARY LOANS - AP') AND CCC_PRODUCT_LINE <> 'PAR LOANS TRADING' AND BOOK NOT IN ('LDN PAR TRADING RG-LNRGO','LDN PAR TRADING SIDDIQUI-LNSSL') AND 
    A.CREDIT_TRADING_FLAG = 'Flow Trading'
GROUP BY A.COB_DATE