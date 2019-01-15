select 
cob_date,
ccc_pl_reporting_region,
Case When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'ITRAXX-EUROPEAN.EU' Then 'iTraxx '||
    ( Case FID1_INDEX_SECTOR When 'CO' Then 'XO' When 'IG' Then 'Main' Else 'Other EU' End)
    When FID1_INDEX_FAMILY = 'ITRAXX-ASIAN' Then 'iTraxx '||
    ( Case When FID1_INDEX_REGION = 'AU' Then 'Australia' When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'ASEXJP.IG' Then 'Asia Ex-Japan IG'
    When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'JP.IG' Then 'Japan IG' Else 'Other Asia' End )
    When FID1_INDEX_FAMILY = 'ITRAXX SOVX' Then 'iTraxx SovX '||
    ( Case  FID1_INDEX_REGION When 'AP' Then 'Asia Pacific' When 'CEEMEA' Then 'CEEMEA' When 'WE' Then 'Western Europe' Else 'Other' End)
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR in ('CDX.NA.HY','CDX.NA.IG')
    Then FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'CDX.EM' Then 'CDX.EM'
    Else 'Other' End
    as INDEX_FAMILY,
sum(Coalesce(USD_CREDIT_GAMMA,0))/10000 as CR_GAMMA,
sum(Coalesce(USD_CR_KAPPA,0)) as CR_VEGA,
sum(Coalesce(USD_PV10_BENCH_COMP,0)) as PV_PLS10,
sum(Coalesce(SLIDE_PVSPRCOMP_PLS_100PCT_USD,0)) as PV_PLS100
    from cdwuser.U_EXP_MSR a
WHERE
    a.COB_DATE IN (
'2017-11-30',
'2017-12-29',
'2018-01-31',
'2018-02-27',
'2018-02-28')
    and BOOK in ('ABOPB','ABOPC','ABOPF','TOLSS','ABOPN','MNSCP','ABOPA','INMGT','ABOPG','ABCHN')
    and PRODUCT_TYPE_CODE in ('CRDINDEX','CDSOPTIDX')
    and EXP_ASSET_TYPE in ('CR','OT')
group by 
cob_date,
ccc_pl_reporting_region,
Case When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'ITRAXX-EUROPEAN.EU' Then 'iTraxx '||
    ( Case FID1_INDEX_SECTOR When 'CO' Then 'XO' When 'IG' Then 'Main' Else 'Other EU' End)
    When FID1_INDEX_FAMILY = 'ITRAXX-ASIAN' Then 'iTraxx '||
    ( Case When FID1_INDEX_REGION = 'AU' Then 'Australia' When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'ASEXJP.IG' Then 'Asia Ex-Japan IG'
    When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'JP.IG' Then 'Japan IG' Else 'Other Asia' End )
    When FID1_INDEX_FAMILY = 'ITRAXX SOVX' Then 'iTraxx SovX '||
    ( Case  FID1_INDEX_REGION When 'AP' Then 'Asia Pacific' When 'CEEMEA' Then 'CEEMEA' When 'WE' Then 'Western Europe' Else 'Other' End)
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR in ('CDX.NA.HY','CDX.NA.IG')
    Then FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'CDX.EM' Then 'CDX.EM'
    Else 'Other' End