With Base as (
Select COB_DATE
, a.STATE_CODE
, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME as CREDIT_ULTIMATE
, sum(Coalesce(USD_EXPOSURE,0))/1000 NET_EXPOSURE_MM
, sum(Coalesce(USD_PV01SPRD,0)) as CS01
from cdwuser.U_CR_MSR a
where A.COB_DATE in ('2018-02-28','2018-01-31')
and CCC_BUSINESS_AREA = 'MUNICIPAL SECURITIES'
and PRODUCT_TYPE_CODE in ('MUNI','MUNI_TAXABLE')
group by COB_DATE, STATE_CODE, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME)

, StateRank as 
(Select STATE_CODE
, Rank() Over (Order by Abs(Sum(NET_EXPOSURE_MM)) Desc) as STATE_RANK
From Base
Where COB_DATE in ('2018-02-28')
and STATE_CODE != 'UNDEFINED'
Group by STATE_CODE)

,NameRank as 
(Select STATE_CODE
, CREDIT_ULTIMATE
, Rank() Over (Partition by STATE_CODE Order by Abs(Sum(NET_EXPOSURE_MM)) Desc) as NAME_RANK
From Base
Where COB_DATE in ('2018-02-28')
Group by STATE_CODE
, CREDIT_ULTIMATE)

Select STATE_RANK||'_State_Rank_'||NAME_RANK||'_Name_Rank' as RANK
, STATE_RANK
, a.COB_DATE
, a.CREDIT_ULTIMATE
, a.STATE_CODE
, a.NET_EXPOSURE_MM
, a.CS01
From Base a
Inner join (Select distinct STATE_CODE, STATE_RANK from StateRank) b on a.STATE_CODE = b.STATE_CODE
Left Join (Select distinct CREDIT_ULTIMATE, STATE_CODE, NAME_RANK from NameRank) c on (a.CREDIT_ULTIMATE, a.STATE_CODE) = (c.CREDIT_ULTIMATE, c.STATE_CODE)
Order by STATE_RANK, NAME_RANK asc