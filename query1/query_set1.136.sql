With Names as ( Select COB_DATE, PROCESS_ID, POSITION_ID, 
CASE WHEN CCC_PRODUCT_LINE = 'DISTRESSED TRADING' and PRODUCT_TYPE_CODE in  ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX','FUTURE','BOND ETF') THEN 'Distressed Hedges'
When CCC_PRODUCT_LINE = 'DISTRESSED TRADING' and CCC_STRATEGY != 'PAR LOANS TRADING' THEN 'Distressed' 
WHEN PRODUCT_TYPE_CODE in ('BOND','PREF') and FID1_SENIORITY in ('AT1','SUBT1','SUBUT2') THEN 'Junior Subs' 
When CCC_PRODUCT_LINE in ('NON IG PRIMARY - HY BOND', 'PRIMARY - BONDS', 'PRIMARY - IG BONDS', 'PRIMARY - NIG BONDS') Then 'Primary - Bonds'
When CCC_PRODUCT_LINE like '%PRIMARY%' Then 'Primary'
When CCC_PRODUCT_LINE = 'CRED CORP PRIMARY FUNDING' Then 'Funding'
WHEN BOOK IN ('ABCHN') then 'Flow Trading'
WHEN CCC_STRATEGY in ('LEGACY MUNI DERIVATIVES','LEGACY BESPOKE CDOS', 'LEGACY FLOW CDS', 'LEGACY HF DERIVS EU',  'LEGACY PORTFOLIO TRADING CDP', 'LEGACY TRANCHED INDEX EU', 'MANAGEMENT OTHER CDP') or CCC_PRODUCT_LINE = 'DSP LEGACY' Then 'DSP Legacy'
WHEN (CCC_PRODUCT_LINE = 'DSP TRS FINANCING' or CCC_STRATEGY = 'DSP TRS FINANCING1') Then 'DSP TRS Financing'
WHEN CCC_PRODUCT_LINE = 'DSP EXOTICS' or CCC_STRATEGY in ('EXOTICS CDP', 'FULL CAP BESPOKE') Then 'DSP Exotics'
Else 'Flow Trading' End
as Credit_Trading_Flag 
, Case When MRD_RATING in ('AAA','AA','A','BBB') THEN 'IG' ELSE 'NIG' END
AS RATING_TYPE
, CASE WHEN a.CCC_PRODUCT_LINE IN ('HIGH YIELD - EU', 'INV GRADE TRADING - EU') THEN 'EUROPEAN CREDIT FLOW' 
  WHEN a.CCC_PRODUCT_LINE IN ('INDEX PROD TRADING - NA', 'DSP INDEX PRODUCTS TRADING') THEN 'DSP INDEX PRODUCTS' ELSE  a.CCC_PRODUCT_LINE END AS CCC_PRODUCT_LINE
, a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, MRD_RATING, TICKET
, MAX(Coalesce(CR_SPREAD_MARK_5Y,0)) as CR_SPREAD_MARK_5Y
, sum(Coalesce(USD_PV10_BENCH,0)) PV10_BENCH
, sum(Coalesce(USD_NOTIONAL,0)) NOTIONAL
, sum(Coalesce(USD_EXPOSURE,0)) NET_EXPOSURE
, sum(Coalesce(USD_INVENTORY,0)) INVENTORY
from cdwuser.U_EXP_MSR a
WHERE
    A.COB_DATE in ('2018-02-28','2018-01-31')
AND
    CCC_BUSINESS_AREA in ('CREDIT-CORPORATES','DSP - CREDIT', 'CREDIT CORPORATES PRIMARY')
    AND CCC_STRATEGY NOT IN ('DSP TRS FINANCING1', 'EXOTICS CDP', 'FULL CAP BESPOKE', 'LEGACY MUNI DERIVATIVES','LEGACY BESPOKE CDOS', 'LEGACY FLOW CDS', 'LEGACY HF DERIVS EU',  'LEGACY PORTFOLIO TRADING CDP', 'LEGACY TRANCHED INDEX EU', 'MANAGEMENT OTHER CDP')
    AND CCC_PRODUCT_LINE NOT IN ('DSP EXOTICS', 'DSP TRS FINANCING', 'DSP LEGACY')
and a.EXP_ASSET_TYPE in ('CR','OT')
group by COB_DATE, PROCESS_ID, POSITION_ID, 
CASE WHEN CCC_PRODUCT_LINE = 'DISTRESSED TRADING' and PRODUCT_TYPE_CODE in  ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX','FUTURE','BOND ETF') THEN 'Distressed Hedges'
When CCC_PRODUCT_LINE = 'DISTRESSED TRADING' and CCC_STRATEGY != 'PAR LOANS TRADING' THEN 'Distressed' 
WHEN PRODUCT_TYPE_CODE in ('BOND','PREF') and FID1_SENIORITY in ('AT1','SUBT1','SUBUT2') THEN 'Junior Subs' 
When CCC_PRODUCT_LINE in ('NON IG PRIMARY - HY BOND', 'PRIMARY - BONDS', 'PRIMARY - IG BONDS', 'PRIMARY - NIG BONDS') Then 'Primary - Bonds'
When CCC_PRODUCT_LINE like '%PRIMARY%' Then 'Primary'
When CCC_PRODUCT_LINE = 'CRED CORP PRIMARY FUNDING' Then 'Funding'
WHEN BOOK IN ('ABCHN') then 'Flow Trading'
WHEN CCC_STRATEGY in ('LEGACY MUNI DERIVATIVES','LEGACY BESPOKE CDOS', 'LEGACY FLOW CDS', 'LEGACY HF DERIVS EU',  'LEGACY PORTFOLIO TRADING CDP', 'LEGACY TRANCHED INDEX EU', 'MANAGEMENT OTHER CDP') or CCC_PRODUCT_LINE = 'DSP LEGACY' Then 'DSP Legacy'
WHEN (CCC_PRODUCT_LINE = 'DSP TRS FINANCING' or CCC_STRATEGY = 'DSP TRS FINANCING1') Then 'DSP TRS Financing'
WHEN CCC_PRODUCT_LINE = 'DSP EXOTICS' or CCC_STRATEGY in ('EXOTICS CDP', 'FULL CAP BESPOKE') Then 'DSP Exotics'
Else 'Flow Trading' End
, Case When MRD_RATING in ('AAA','AA','A','BBB') THEN 'IG' ELSE 'NIG' END
, CASE WHEN a.CCC_PRODUCT_LINE IN ('HIGH YIELD - EU', 'INV GRADE TRADING - EU') THEN 'EUROPEAN CREDIT FLOW' 
  WHEN a.CCC_PRODUCT_LINE IN ('INDEX PROD TRADING - NA', 'DSP INDEX PRODUCTS TRADING') THEN 'DSP INDEX PRODUCTS' ELSE  a.CCC_PRODUCT_LINE END
, a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, MRD_RATING, TICKET 
Having sum(Coalesce(USD_PV10_BENCH,0)) + sum(Coalesce(USD_EXPOSURE,0)) + sum(Coalesce(USD_INVENTORY,0)) != 0 )

Select RANK_TYPE, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME as CREDIT_ULTIMATE, CCC_PRODUCT_LINE, CR_SPREAD_MARK_5Y, MRD_RATING
, PV10, PV10_DOD, EXPOSURE, EXPOSURE_DOD, NOTIONAL, NOTIONAL_DOD
from ( 
--IG - Identifies top position per IG Name (rating >= BBB) by Pv10 and aggregates all risk on that name (ex. Primary, Distressed, and Junior Subordinated debt)
Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And RATING_TYPE = 'IG' THEN PV10_BENCH Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And RATING_TYPE = 'IG' THEN PV10_BENCH Else 0 End)) DESC)||'_'||'IG_Spread' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else 0 END) as PV10
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else -PV10_BENCH END) as PV10_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV10_BENCH) as PV10
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(PV10_BENCH)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag in ('Flow Trading','Primary - Bonds') And RATING_TYPE = 'IG'
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag in ('Flow Trading','Primary - Bonds')
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    UNION ALL
--NIG - Identifies top position per NIG name (rating < BBB) and aggregates all risk on that name (ex. Primary, Distressed, and Junior Subordinated debt)
    Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And RATING_TYPE = 'NIG' THEN PV10_BENCH Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And RATING_TYPE = 'NIG' THEN PV10_BENCH Else 0 End)) DESC)||'_'||'NIG_Spread' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else 0 END) as PV10
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else -PV10_BENCH END) as PV10_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV10_BENCH) as PV10
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(PV10_BENCH)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag in ('Flow Trading','Primary - Bonds') And RATING_TYPE = 'NIG'
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag in ('Flow Trading','Primary - Bonds')
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    UNION ALL
--IG - Identifies top position in IG and aggregates all risk on that Name (ex. Primary, Distressed, and Junior Subordinated debt)
    Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE in ('INV GRADE TRADING - NA') THEN PV10_BENCH Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE in ('INV GRADE TRADING - NA') THEN PV10_BENCH Else 0 End)) DESC)||'_'||'IG_Desk' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else 0 END) as PV10
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else -PV10_BENCH END) as PV10_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV10_BENCH) as PV10
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(PV10_BENCH)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag in ('Flow Trading') And CCC_PRODUCT_LINE in ('INV GRADE TRADING - NA')
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag = 'Flow Trading'
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    UNION ALL
--HY - Identifies top position in HY and aggregates all risk on that Name (ex. Primary, Distressed, and Junior Subordinated debt)
    Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE in ('HIGH YIELD - NA') THEN PV10_BENCH Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' And a.CCC_PRODUCT_LINE in ('HIGH YIELD - NA') THEN PV10_BENCH Else 0 End)) DESC)||'_'||'HY_Desk' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else 0 END) as PV10
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else -PV10_BENCH END) as PV10_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV10_BENCH) as PV10
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(PV10_BENCH)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag in ('Flow Trading') And CCC_PRODUCT_LINE in ('HIGH YIELD - NA')
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag = 'Flow Trading'
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    UNION ALL
--Junior Subs - Identifies top position per Junior Sub name (AT1, SubT1, SubUT2) and aggregates all risk on that name (ignores non-Junior Subordinated debt on same name)
    Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' and Credit_Trading_Flag = 'Junior Subs' THEN NET_EXPOSURE Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' and Credit_Trading_Flag = 'Junior Subs' THEN NET_EXPOSURE Else 0 End)) DESC)||'_'||'Junior_Subs' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else 0 END) as PV10
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else -PV10_BENCH END) as PV10_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV10_BENCH) as PV10
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(NET_EXPOSURE)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag = 'Junior Subs'
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag = 'Junior Subs'
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    UNION ALL
--Distressed - Identifies top names in Distressed, ignores exposure to names traded outside of Distressed (included in other populations)
    Select Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' and Credit_Trading_Flag = 'Distressed' THEN NET_EXPOSURE Else 0 End)) DESC) as RANK
    , Rank() Over (Order by ABS(SUM(Case when COB_DATE = '2018-02-28' and Credit_Trading_Flag = 'Distressed' THEN NET_EXPOSURE Else 0 End)) DESC)||'_'||'Distressed' as RANK_TYPE
    , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    , b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else 0 END) as PV10
    , SUM(Case When COB_DATE = '2018-02-28' THEN PV10_BENCH Else -PV10_BENCH END) as PV10_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else 0 END) as EXPOSURE
    , SUM(Case When COB_DATE = '2018-02-28' THEN NET_EXPOSURE Else -NET_EXPOSURE END) as EXPOSURE_DOD
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else 0 END) as NOTIONAL
    , SUM(Case When COB_DATE = '2018-02-28' THEN NOTIONAL Else -NOTIONAL END) as NOTIONAL_DOD
    From Names a 
    inner join 
        (Select * from (Select POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y, SUM(PV10_BENCH) as PV10
        , Rank() Over (Partition by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME order by ABS(SUM(INVENTORY)) DESC) as POSITION_RANK
        from Names a 
        where COB_DATE in ('2018-02-28')
        and Credit_Trading_Flag = 'Distressed'
        group by POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, CCC_PRODUCT_LINE, MRD_RATING, CR_SPREAD_MARK_5Y) sub_qry Where POSITION_RANK = 1) b
    on a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    Where Credit_Trading_Flag = 'Distressed'
    Group by a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, b.CCC_PRODUCT_LINE, b.CR_SPREAD_MARK_5Y, b.MRD_RATING
    ) sub_qry 
Where RANK <= 10