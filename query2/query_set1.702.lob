With Names as ( 
Select 
COB_DATE,  
a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,  
sum(coalesce(a.USD_PV10_BENCH,0)) as PV10,
sum(coalesce(a.USD_DEFAULT_PNL,0)) as JTD,
sum(coalesce(a.USD_PV01SPRD,0)) as SPV01
from cdwuser.U_EXP_MSR a
WHERE
    A.COB_DATE in ('2018-02-28','2018-02-27')
and CCC_BUSINESS_AREA in ('DSP - CREDIT') 
and a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME != 'UNDEFINED'
group by 
COB_DATE, 
a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
Having sum(Coalesce(a.USD_PV10_BENCH,0)) + sum(Coalesce(a.USD_DEFAULT_PNL,0)) + sum(Coalesce(a.USD_PV01SPRD,0))!= 0 
)

Select * from (
Select 
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN PV10 Else 0 End)) DESC) as RANK,
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN PV10 Else 0 End)) DESC)||'_'||'PV10_SHORT' as RANK_TYPE,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,    
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else 0 END) as PV10,
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else -PV10 END) as PV10_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else 0 END) as JTD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else -JTD END) as JTD_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else 0 END) as SPV01,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else -SPV01 END) as SPV01_DOD
    From Names a     
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME

union all
Select 
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN PV10 Else 0 End))) as RANK,
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN PV10 Else 0 End)))||'_'||'PV10_LONG' as RANK_TYPE,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,    
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else 0 END) as PV10,
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else -PV10 END) as PV10_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else 0 END) as JTD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else -JTD END) as JTD_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else 0 END) as SPV01,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else -SPV01 END) as SPV01_DOD
    From Names a     
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME

union all
Select 
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN SPV01 Else 0 End)) DESC) as RANK,
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN SPV01 Else 0 End)) DESC)||'_'||'SPV01_SHORT' as RANK_TYPE,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,    
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else 0 END) as PV10,
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else -PV10 END) as PV10_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else 0 END) as JTD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else -JTD END) as JTD_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else 0 END) as SPV01,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else -SPV01 END) as SPV01_DOD
    From Names a     
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME

union all
Select 
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN SPV01 Else 0 End))) as RANK,
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN SPV01 Else 0 End)))||'_'||'SPV01_LONG' as RANK_TYPE,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,    
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else 0 END) as PV10,
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else -PV10 END) as PV10_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else 0 END) as JTD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else -JTD END) as JTD_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else 0 END) as SPV01,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else -SPV01 END) as SPV01_DOD
    From Names a     
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME

union all
Select 
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN JTD Else 0 End)) DESC) as RANK,
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN JTD Else 0 End)) DESC)||'_'||'JTD_SHORT' as RANK_TYPE,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,    
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else 0 END) as PV10,
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else -PV10 END) as PV10_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else 0 END) as JTD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else -JTD END) as JTD_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else 0 END) as SPV01,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else -SPV01 END) as SPV01_DOD
    From Names a     
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME

union all
Select 
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN JTD Else 0 End))) as RANK,
    Rank() Over (Order by (SUM(Case when COB_DATE = '2018-02-28' THEN JTD Else 0 End)))||'_'||'JTD_LONG' as RANK_TYPE,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,    
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else 0 END) as PV10,
    SUM(Case When COB_DATE = '2018-02-28' THEN PV10 Else -PV10 END) as PV10_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else 0 END) as JTD,
    SUM(Case When COB_DATE = '2018-02-28' THEN JTD Else -JTD END) as JTD_DOD,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else 0 END) as SPV01,
    SUM(Case When COB_DATE = '2018-02-28' THEN SPV01 Else -SPV01 END) as SPV01_DOD
    From Names a     
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
 ) sub_qry 
Where RANK <= 5