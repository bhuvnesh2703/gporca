With Names as ( Select COB_DATE, PROCESS_ID, POSITION_ID, CASE WHEN CCC_PRODUCT_LINE = 'DISTRESSED TRADING' and PRODUCT_TYPE_CODE in ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX','FUTURE','BOND ETF') THEN 'Distressed Hedges'
When CCC_PRODUCT_LINE = 'DISTRESSED TRADING' and CCC_STRATEGY != 'PAR LOANS TRADING' THEN 'Distressed'
When CCC_PRODUCT_LINE like '%PRIMARY%' Then 'Primary'
WHEN PRODUCT_TYPE_CODE in ('BOND','PREF') and FID1_SENIORITY in ('AT1','SUBT1','SUBUT2') THEN 'Junior Subs'
Else 'Flow Trading' End
as Credit_Trading_Flag 
, Case When MRD_RATING in ('AAA','AA','A','BBB') THEN 'IG' ELSE 'NIG' END
AS RATING_TYPE
, CASE WHEN a.CCC_PRODUCT_LINE IN ('HIGH YIELD - EU', 'INV GRADE TRADING - EU') THEN 'EUROPEAN CREDIT FLOW' 
  WHEN a.CCC_PRODUCT_LINE IN ('INDEX PROD TRADING - NA', 'DSP INDEX PRODUCTS TRADING') THEN 'DSP INDEX PRODUCTS' ELSE  a.CCC_PRODUCT_LINE END AS CCC_PRODUCT_LINE
,  a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, MRD_RATING, TICKET
, MAX(Coalesce(CR_SPREAD_MARK_5Y,0)) as CR_SPREAD_MARK_5Y
, sum(Coalesce(USD_PV01SPRD,0)) PV01SPRD
, sum(Coalesce(USD_NOTIONAL,0)) NOTIONAL
, sum(Coalesce(USD_EXPOSURE,0)) NET_EXPOSURE
, sum(Coalesce(USD_INVENTORY,0)) INVENTORY
from cdwuser.U_EXP_MSR a
WHERE
    A.COB_DATE in ('2018-02-28','2018-02-27')
and CCC_BUSINESS_AREA = 'CREDIT-CORPORATES'
and a.EXP_ASSET_TYPE in ('CR','OT')
group by COB_DATE, PROCESS_ID, POSITION_ID, CASE WHEN CCC_PRODUCT_LINE = 'DISTRESSED TRADING' and PRODUCT_TYPE_CODE in ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX','FUTURE','BOND ETF') THEN 'Distressed Hedges'
When CCC_PRODUCT_LINE = 'DISTRESSED TRADING' and CCC_STRATEGY != 'PAR LOANS TRADING' THEN 'Distressed'
When CCC_PRODUCT_LINE like '%PRIMARY%' Then 'Primary'
WHEN PRODUCT_TYPE_CODE in ('BOND','PREF') and FID1_SENIORITY in ('AT1','SUBT1','SUBUT2') THEN 'Junior Subs'
Else 'Flow Trading' End, Case When MRD_RATING in ('AAA','AA','A','BBB') THEN 'IG' ELSE 'NIG' END
, CASE WHEN a.CCC_PRODUCT_LINE IN ('HIGH YIELD - EU', 'INV GRADE TRADING - EU') THEN 'EUROPEAN CREDIT FLOW' 
  WHEN a.CCC_PRODUCT_LINE IN ('INDEX PROD TRADING - NA', 'DSP INDEX PRODUCTS TRADING') THEN 'DSP INDEX PRODUCTS' ELSE  a.CCC_PRODUCT_LINE END
, a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, MRD_RATING, TICKET 
Having sum(Coalesce(USD_PV01SPRD,0)) + sum(Coalesce(USD_EXPOSURE,0)) + sum(Coalesce(USD_INVENTORY,0)) != 0 )

Select RANK_TYPE, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME as CREDIT_ULTIMATE, CCC_PRODUCT_LINE, CR_SPREAD_MARK_5Y, MRD_RATING
, PV01, PV01_DOD, EXPOSURE, EXPOSURE_DOD, NOTIONAL, NOTIONAL_DOD
from ( 
--IG - Identifies top position per IG Name (rating >= BBB) by Pv10 and aggregates all risk on that name (ex. Primary, Distressed, and Junior Subordinated debt)
Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE in ('INV GRADE TRADING - NA') THEN PV01SPRD Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE in ('INV GRADE TRADING - NA') THEN PV01SPRD Else 0 End)) DESC)||'_'||'IG_Desk' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV01SPRD Else 0 END) as PV01
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV01SPRD Else -PV01SPRD END) as PV01_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV01SPRD) as PV01
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(PV01SPRD)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag = 'Flow Trading' And CCC_PRODUCT_LINE in ('INV GRADE TRADING - NA')
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag = 'Flow Trading'
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    UNION ALL
--HY - Identifies top position in HY and aggregates all risk on that Name (ex. Primary, Distressed, and Junior Subordinated debt)
    Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE in ('HIGH YIELD - NA') THEN PV01SPRD Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE in ('HIGH YIELD - NA') THEN PV01SPRD Else 0 End)) DESC)||'_'||'HY_Desk' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV01SPRD Else 0 END) as PV01
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV01SPRD Else -PV01SPRD END) as PV01_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV01SPRD) as PV01
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(PV01SPRD)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag = 'Flow Trading' And CCC_PRODUCT_LINE in ('HIGH YIELD - NA')
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag = 'Flow Trading'
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    UNION ALL
--ACT - Identifies top position per ACT name (rating < BBB) and aggregates all risk on that name (ex. Primary, Distressed, and Junior Subordinated debt)
    Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE = 'ASIA CREDIT TRADING' THEN PV01SPRD Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE = 'ASIA CREDIT TRADING' THEN PV01SPRD Else 0 End)) DESC)||'_'||'ACT_Desk' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV01SPRD Else 0 END) as PV01
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV01SPRD Else -PV01SPRD END) as PV01_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV01SPRD) as PV01
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(PV01SPRD)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag = 'Flow Trading' And CCC_PRODUCT_LINE = 'ASIA CREDIT TRADING'
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag = 'Flow Trading'
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
     UNION ALL
--ECF - Identifies top position per ACT name (rating < BBB) and aggregates all risk on that name (ex. Primary, Distressed, and Junior Subordinated debt)
    Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE IN ('HIGH YIELD - EU', 'INV GRADE TRADING - EU', 'EUROPEAN CREDIT FLOW') THEN PV01SPRD Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE IN ('HIGH YIELD - EU', 'INV GRADE TRADING - EU', 'EUROPEAN CREDIT FLOW') THEN PV01SPRD Else 0 End)) DESC)||'_'||'ECF_Desk' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV01SPRD Else 0 END) as PV01
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV01SPRD Else -PV01SPRD END) as PV01_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV01SPRD) as PV01
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(PV01SPRD)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag = 'Flow Trading' And CCC_PRODUCT_LINE IN ('HIGH YIELD - EU', 'INV GRADE TRADING - EU', 'EUROPEAN CREDIT FLOW')
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag = 'Flow Trading'
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    ) sub_qry 
Where RANK <= 10