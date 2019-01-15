Select
    A.COB_DATE,
    Case When substr(Book,length(Book)+1-5) = 'PARCW' or CCC_STRATEGY = 'PAR CLO' Then 'CLO Warehouse'
    When FACILITY_TYPE = 'TERM LOAN A' Then 'Term Loan A'
    When FACILITY_TYPE = 'TERM LOAN B/1ST LIEN' Then 'Term Loan B'
    When FACILITY_TYPE like 'TERM LOAN%' Then 'Term Loan Other'
    When FACILITY_TYPE = 'TERM MULTI-DRAW' Then 'Term Loan Other' 
    When FACILITY_TYPE like 'REVOLVER%' or FACILITY_TYPE like '%RCF%' Then 'Revolver'
    when FACILITY_TYPE in ('LOC', 'LETTER OF CREDIT/STANDBY (TERM)','LETTER OF CREDIT/STANDBY (REVOLVING)','RBL/LOC RCF FOR RESERVED BASED LENDING') Then 'LOC'
    When FACILITY_TYPE like '%PIK%' Then 'PIK' 
    Else 'Other' end
    AS FACILITY_TYPE_BUCKET,
    SUM (A.USD_EXPOSURE) AS NET_EXPOSURE,
    SUM (A.USD_PV10_BENCH) AS USD_PV10_BENCH,
    SUM (A.USD_NOTIONAL) AS NOTIONAL
from cdwuser.U_EXP_MSR a
where 
    A.COB_DATE in ('2018-02-28','2018-02-27') AND
    A.CCC_BUSINESS_AREA IN ('CREDIT-CORPORATES') AND
    (CCC_STRATEGY in ('PRIMARY LOANS - AP','SECONDARY LOANS - AP') OR CCC_PRODUCT_LINE = 'PAR LOANS TRADING')
AND A.CCC_TAPS_COMPANY in ('1633')
group by 
    A.COB_DATE,
    Case When substr(Book,length(Book)+1-5) = 'PARCW' or CCC_STRATEGY = 'PAR CLO' Then 'CLO Warehouse'
    When FACILITY_TYPE = 'TERM LOAN A' Then 'Term Loan A'
    When FACILITY_TYPE = 'TERM LOAN B/1ST LIEN' Then 'Term Loan B'
    When FACILITY_TYPE like 'TERM LOAN%' Then 'Term Loan Other'
    When FACILITY_TYPE = 'TERM MULTI-DRAW' Then 'Term Loan Other' 
    When FACILITY_TYPE like 'REVOLVER%' or FACILITY_TYPE like '%RCF%' Then 'Revolver'
    when FACILITY_TYPE in ('LOC', 'LETTER OF CREDIT/STANDBY (TERM)','LETTER OF CREDIT/STANDBY (REVOLVING)','RBL/LOC RCF FOR RESERVED BASED LENDING') Then 'LOC'
    When FACILITY_TYPE like '%PIK%' Then 'PIK' 
    Else 'Other' end