With
x as (Select COB_DATE, BOOK, PRODUCT_TYPE_CODE, CONSOLIDATED_RATING,
Case when GICS_LEVEL_1_NAME in ('FINANCIALS','REAL ESTATE') then 'Fins'
when GICS_LEVEL_1_NAME in ('ENERGY', 'MATERIALS', 'UTILITIES') Then 'Energy' 
when GICS_LEVEL_1_NAME in ('TELECOMMUNICATION SERVICES', 'INFORMATION TECHNOLOGY') Then 'IT' 
Else GICS_LEVEL_1_NAME END  as Industry,
Case When a.PRODUCT_TYPE_CODE in ('BANKDEBT') then 'Loan' else 'Hedge' end as LOAN_TYPE,
a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME as Name,
Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - PCIF ELTH WORKOUT HFI MSBNA-LPFWB'))Then 'HFI'
when BOOK like 'HFI%' Then 'HFI' Else 'Other' END as Level6,
sum(CR_SPREAD_MARK_5Y * abs(USD_PV10_BENCH)) as SPREAD,
sum(abs(USD_PV10_BENCH)) as sum_ABS_PV10,
sum(coalesce(USD_PV01SPRD,0)) pv01,
sum(coalesce(USD_PV10_BENCH,0)) pv10,
Case when PRODUCT_TYPE_CODE in ('CDSOPTIDX', 'CRDINDEX') Then sum(coalesce(SLIDE_PVSPRCOMP_PLS_50PCT_USD ,0)) ELSE sum(coalesce(a.SLIDE_PV_PLS_50PCT_USD,0)) END as PV50,
Case when PRODUCT_TYPE_CODE in ('CDSOPTIDX', 'CRDINDEX') Then sum(coalesce(SLIDE_PVSPRCOMP_PLS_100PCT_USD ,0)) ELSE sum(coalesce(a.SLIDE_PV_PLS_100PCT_USD,0)) END as PV100,
Case when PRODUCT_TYPE_CODE in ('CDSOPTIDX', 'CRDINDEX') Then sum(coalesce(SLIDE_PVSPRCOMP_PLS_200PCT_USD ,0)) ELSE sum(coalesce(a.SLIDE_PV_PLS_200PCT_USD,0)) END as PV200,
sum(coalesce(a.USD_EXPOSURE,0)) NET_EXPOSURE,
sum(coalesce(a.USD_NOTIONAL,0)) NOTIONAL
from cdwuser.U_EXP_MSR a
where
    a.COB_DATE IN 
    ('2018-02-28','2018-02-27')
and (CCC_BUSINESS_AREA = 'LENDING' or (CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' and CCC_PRODUCT_LINE = 'PRIMARY - LOANS'))
and CCC_STRATEGY not in ('CORPORATE LOAN STRATEGY','PROJECT FINANCE','EVENT - INV GRADE','EVENT - NON INV GRADE')
and a.VERTICAL_SYSTEM not like 'PIPELINE%'
and a.BOOK like '%HFI%'
AND CCC_TAPS_COMPANY in ('1633')
Group by COB_DATE, BOOK, PRODUCT_TYPE_CODE, CONSOLIDATED_RATING,
Case when GICS_LEVEL_1_NAME in ('FINANCIALS','REAL ESTATE') then 'Fins'
when GICS_LEVEL_1_NAME in ('ENERGY', 'MATERIALS', 'UTILITIES') Then 'Energy' 
when GICS_LEVEL_1_NAME in ('TELECOMMUNICATION SERVICES', 'INFORMATION TECHNOLOGY') Then 'IT' 
Else GICS_LEVEL_1_NAME END,
Case When a.PRODUCT_TYPE_CODE in ('BANKDEBT') then 'Loan' else 'Hedge' end,
a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - PCIF ELTH WORKOUT HFI MSBNA-LPFWB'))Then 'HFI'
when BOOK like 'HFI%' Then 'HFI' Else 'Other' END),

z as (Select COB_DATE, Level6, Name
, rank() over (Partition by Level6 order by ABS(sum(pv10)) Desc) AS rank
, sum(pv10) pv10
from x
where
    COB_DATE IN 
    ('2018-02-28')
Group by COB_DATE, Level6, Name)

Select x.COB_DATE, x.Level6, x.Product_Type_Code, x.Loan_Type, x.CONSOLIDATED_RATING, x.industry,
Case when coalesce(z.rank,11)<10 then 0||rank
when z.rank=10 then '10'
else '11' end as dense_rank,
Case when coalesce(z.rank,12) < 11 Then x.Name Else 'Other' END as Topname 
, sum(x.spread) spread
, sum(x.sum_ABS_PV10) sum_ABS_PV10
, sum(x.pv01) pv01
, sum(x.pv10) pv10
, sum(x.pv50) pv50
, sum(x.pv100) pv100
, sum(x.pv200) pv200
, sum(NET_EXPOSURE) NET_EXPOSURE
, sum(NOTIONAL) NOTIONAL
from x left join z on (x.Level6,x.Name) = (z.Level6,z.Name)
where x.level6 in ('HFI')
Group by x.COB_DATE, x.Level6, x.Product_Type_Code, x.Loan_Type, x.CONSOLIDATED_RATING, x.industry,
Case when coalesce(z.rank,11)<10 then 0||rank
when z.rank=10 then '10'
else '11' end,
Case when coalesce(z.rank,12) < 11 Then x.Name Else 'Other' END