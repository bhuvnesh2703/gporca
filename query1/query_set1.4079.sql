Select
    A.COB_DATE,
    Case When substr(Book,length(Book)+1-5) = 'PARCW' or CCC_STRATEGY = 'PAR CLO' Then 'CLO Warehouse'
    Else CCC_PL_REPORTING_REGION End
    AS CCC_PL_REPORTING_REGION,
    SUM(A.USD_NET_EXPOSURE) AS NET_EXPOSURE
from cdwuser.U_DM_CC a
where
    a.COB_DATE IN (
'2017-10-31',
'2017-11-30',
'2017-12-29',
'2018-01-31',
'2018-02-28')
    AND A.CCC_BUSINESS_AREA IN ('CREDIT-CORPORATES')
    AND (CCC_STRATEGY in ('PRIMARY LOANS - AP','SECONDARY LOANS - AP') OR CCC_PRODUCT_LINE = 'PAR LOANS TRADING')

group by 
    A.COB_DATE,
    Case When substr(Book,length(Book)+1-5) = 'PARCW' or CCC_STRATEGY = 'PAR CLO' Then 'CLO Warehouse'
    Else CCC_PL_REPORTING_REGION End