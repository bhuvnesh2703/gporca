SELECT
    A.COB_DATE,
    SUM (A.USD_PV10_BENCH) AS USD_PV10_BENCH
FROM cdwuser.U_DM_CC A
WHERE
    a.COB_DATE IN (
'2018-01-24', 
'2018-01-31', 
'2018-02-07', 
'2018-02-14', 
'2018-02-21', 
'2018-02-27', 
'2018-02-28'
) AND
    A.CREDIT_CORPS_CCC_BUSINESS_AREA IN ('CREDIT-CORPORATES') AND
    A.PARENT_LEGAL_ENTITY IN ('0302(S)', '0342', '0517(G)', '0302(G)', '0517(S)', '0621(S)') AND 

    A.CCC_STRATEGY NOT LIKE '%PAR LOANS TRADING%' AND CCC_STRATEGY NOT in ('PRIMARY LOANS - AP','SECONDARY LOANS - AP') AND CCC_PRODUCT_LINE <> 'PAR LOANS TRADING' AND BOOK NOT IN ('LDN PAR TRADING RG-LNRGO','LDN PAR TRADING SIDDIQUI-LNSSL') AND 
    A.CREDIT_TRADING_FLAG = 'Flow Trading'
GROUP BY A.COB_DATE