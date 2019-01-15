Select COB_DATE, OPT_BUY_SELL, OPT_TYPE, STRIKE, INDEX_FAMILY, INDEX, CR_OPTION_EXPIRY_DATE, MONEYNESS_BUCKET, REFERENCE_INDEX_ENTITY_NAME, OPTION_EXPIRY_BUCKET
, sum(OPTION_POSITION) as OPTION_POSITION
, sum(NOTIONAL) as NOTIONAL
, sum(EXPOSURE) as EXPOSURE
, sum(CR_GAMMA) as CR_GAMMA
, sum(CR_VEGA) as CR_VEGA
, sum(PV_MIN50) as PV_MIN50, sum(PV_MIN10) as PV_MIN10, sum(PV_PLS10) as PV_PLS10
, sum(PV_PLS50) as PV_PLS50, sum(PV_PLS100) as PV_PLS100, sum(PV_PLS200) as PV_PLS200, sum(PV_PLS300) as PV_PLS300, sum(PV_PLS400) as PV_PLS400
from
    (Select COB_DATE, TICKET, PRODUCT_TYPE_CODE, REFERENCE_INDEX_ENTITY_NAME, STRIKE, CR_OPTION_EXPIRY_DATE
    , FID1_INDEX_FAMILY, FID1_INDEX_SECTOR, FID1_INDEX_REGION
    , Case When PRODUCT_TYPE_CODE = 'CRDINDEX' Then 'Index'
    When CR_OPTION_EXPIRY_DATE - COB_DATE <= 30 Then '0-1M'
    When CR_OPTION_EXPIRY_DATE - COB_DATE <= 60 Then '1-2M'
    When CR_OPTION_EXPIRY_DATE - COB_DATE <= 90 Then '2-3M'
    When CR_OPTION_EXPIRY_DATE - COB_DATE <= 120 Then '3-4M'
    Else '4M+' End
    as OPTION_EXPIRY_BUCKET
    , Case When PRODUCT_TYPE_CODE = 'CRDINDEX' Then 'Index' 
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 <= -40 Then '<=-40%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = -30 Then '-30%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = -20 Then '-20%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = -10 Then '-10%'
    When Round(MONEYNESS_PERCENTAGE*1000,0) > -75 and Round(MONEYNESS_PERCENTAGE*1000,0) <= -25 Then '-5%'
    When Round(MONEYNESS_PERCENTAGE*1000,0) > -25 and Round(MONEYNESS_PERCENTAGE*1000,0) < 25 Then '0'
    When Round(MONEYNESS_PERCENTAGE*1000,0) >= 25 and Round(MONEYNESS_PERCENTAGE*1000,0) < 75 Then '+5%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = 10 Then '+10%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = 20 Then '+20%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = 30 Then '+30%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 >= 40 Then '>=40%'
    Else '0' End 
    as MONEYNESS_BUCKET
    , Case When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'ITRAXX-EUROPEAN.EU' Then 'iTraxx '||
    ( Case FID1_INDEX_SECTOR When 'CO' Then 'XO S' When 'IG' Then 'Main S' Else 'Other EU S' End)||FID1_INDEX_SERIES
    When FID1_INDEX_FAMILY = 'ITRAXX-ASIAN' Then 'iTraxx '||
    ( Case When FID1_INDEX_REGION = 'AU' Then 'Australia S' When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'ASEXJP.IG' Then 'Asia Ex-Japan IG S'
    When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'JP.IG' Then 'Japan IG S' Else 'Other Asia S' End )||FID1_INDEX_SERIES
    When FID1_INDEX_FAMILY = 'ITRAXX SOVX' Then 'iTraxx SovX '||
    ( Case  FID1_INDEX_REGION When 'AP' Then 'Asia Pacific S' When 'CEEMEA' Then 'CEEMEA S' When 'WE' Then 'Western Europe S' Else 'Other S' End)||FID1_INDEX_SERIES
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR in ('CDX.NA.HY','CDX.NA.IG')
    Then FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR||' S'||FID1_INDEX_SERIES
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'CDX.EM' Then 'CDX.EM S'||FID1_INDEX_SERIES
    Else 'Other' End
    as INDEX
    , Case When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'ITRAXX-EUROPEAN.EU' Then 'iTraxx '||
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
    as INDEX_FAMILY
    , sum(Coalesce(USD_CREDIT_GAMMA,0))/10000 as CR_GAMMA
    , sum(Coalesce(USD_CR_KAPPA,0)) as CR_VEGA
    , sum(Coalesce(USD_NOTIONAL,0)) as NOTIONAL
    , sum(Coalesce(USD_OPTION_POSITION,0)) as OPTION_POSITION
    , sum(Coalesce(USD_EXPOSURE_COMP,0)) as EXPOSURE
    , Case When PRODUCT_TYPE_CODE = 'CRDINDEX' Then 'Index' When sum(Coalesce(USD_OPTION_POSITION,0)) > 0 Then 'Buy' Else 'Sell' End as OPT_BUY_SELL
    , Case When PRODUCT_TYPE_CODE = 'CRDINDEX' Then 'Index' When sum(Coalesce(USD_OPTION_POSITION,0)) * sum(Coalesce(USD_NOTIONAL,0)) > 0 Then 'Receiver' Else 'Payer' End as OPT_TYPE
    , sum(Coalesce(SLIDE_PVSPRCOMP_MIN_50PCT_USD,0)) as PV_MIN50
    , sum(Coalesce(SLIDE_PVSPRCOMP_MIN_10PCT_USD,0)) as PV_MIN10
    , sum(Coalesce(USD_PV10_BENCH_COMP,0)) as PV_PLS10
    , sum(Coalesce(SLIDE_PVSPRCOMP_PLS_50PCT_USD,0)) as PV_PLS50
    , sum(Coalesce(SLIDE_PVSPRCOMP_PLS_100PCT_USD,0)) as PV_PLS100
    , sum(Coalesce(SLIDE_PVSPRCOMP_PLS_200PCT_USD,0)) as PV_PLS200
    , sum(Coalesce(SLIDE_PVSPRCOMP_PLS_300PCT_USD,0)) as PV_PLS300
    , sum(Coalesce(SLIDE_PVSPRCOMP_PLS_400PCT_USD,0)) as PV_PLS400
    from cdwuser.U_EXP_MSR a
where
    a.COB_DATE IN 
    ('2018-02-28')
    and PRODUCT_TYPE_CODE in ('CRDINDEX','CDSOPTIDX')
    and EXP_ASSET_TYPE in ('CR','OT')
    and CCC_BUSINESS_AREA='SECURITIZED PRODUCTS GRP'
    Group by COB_DATE, TICKET, PRODUCT_TYPE_CODE, REFERENCE_INDEX_ENTITY_NAME, STRIKE, CR_OPTION_EXPIRY_DATE
    , FID1_INDEX_FAMILY, FID1_INDEX_SECTOR, FID1_INDEX_REGION
    , Case When PRODUCT_TYPE_CODE = 'CRDINDEX' Then 'Index'
    When CR_OPTION_EXPIRY_DATE - COB_DATE <= 30 Then '0-1M'
    When CR_OPTION_EXPIRY_DATE - COB_DATE <= 60 Then '1-2M'
    When CR_OPTION_EXPIRY_DATE - COB_DATE <= 90 Then '2-3M'
    When CR_OPTION_EXPIRY_DATE - COB_DATE <= 120 Then '3-4M'
    Else '4M+' End
    , Case When PRODUCT_TYPE_CODE = 'CRDINDEX' Then 'Index' 
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 <= -40 Then '<=-40%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = -30 Then '-30%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = -20 Then '-20%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = -10 Then '-10%'
    When Round(MONEYNESS_PERCENTAGE*1000,0) > -75 and Round(MONEYNESS_PERCENTAGE*1000,0) <= -25 Then '-5%'
    When Round(MONEYNESS_PERCENTAGE*1000,0) > -25 and Round(MONEYNESS_PERCENTAGE*1000,0) < 25 Then '0'
    When Round(MONEYNESS_PERCENTAGE*1000,0) >= 25 and Round(MONEYNESS_PERCENTAGE*1000,0) < 75 Then '+5%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = 10 Then '+10%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = 20 Then '+20%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 = 30 Then '+30%'
    When Round(MONEYNESS_PERCENTAGE*10,0)*10 >= 40 Then '>=40%'
    Else '0' End 
    , Case When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'ITRAXX-EUROPEAN.EU' Then 'iTraxx '||
    ( Case FID1_INDEX_SECTOR When 'CO' Then 'XO S' When 'IG' Then 'Main S' Else 'Other EU S' End)||FID1_INDEX_SERIES
    When FID1_INDEX_FAMILY = 'ITRAXX-ASIAN' Then 'iTraxx '||
    ( Case When FID1_INDEX_REGION = 'AU' Then 'Australia S' When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'ASEXJP.IG' Then 'Asia Ex-Japan IG S'
    When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'JP.IG' Then 'Japan IG S' Else 'Other Asia S' End )||FID1_INDEX_SERIES
    When FID1_INDEX_FAMILY = 'ITRAXX SOVX' Then 'iTraxx SovX '||
    ( Case  FID1_INDEX_REGION When 'AP' Then 'Asia Pacific S' When 'CEEMEA' Then 'CEEMEA S' When 'WE' Then 'Western Europe S' Else 'Other S' End)||FID1_INDEX_SERIES
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR in ('CDX.NA.HY','CDX.NA.IG')
    Then FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR||' S'||FID1_INDEX_SERIES
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'CDX.EM' Then 'CDX.EM S'||FID1_INDEX_SERIES
    Else 'Other' End
    , Case When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'ITRAXX-EUROPEAN.EU' Then 'iTraxx '||
    ( Case FID1_INDEX_SECTOR When 'CO' Then 'XO' When 'IG' Then 'Main' Else 'Other EU' End)
    When FID1_INDEX_FAMILY = 'ITRAXX-ASIAN' Then 'iTraxx '||
    ( Case When FID1_INDEX_REGION = 'AU' Then 'Australia' When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'ASEXJP.IG' Then 'Asia Ex-Japan IG'
    When FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR = 'JP.IG' Then 'Japan IG' Else 'Other Asia' End )
    When FID1_INDEX_FAMILY = 'ITRAXX SOVX' Then 'iTraxx SovX '||
    ( Case  FID1_INDEX_REGION When 'AP' Then 'Asia Pacific' When 'CEEMEA' Then 'CEEMEA' When 'WE' Then 'Western Europe' Else 'Other' End)
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR in ('CDX.NA.HY','CDX.NA.IG')
    Then FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION||'.'||FID1_INDEX_SECTOR
    When FID1_INDEX_FAMILY ||'.'||FID1_INDEX_REGION = 'CDX.EM' Then 'CDX.EM'
    Else 'Other' End ) a
Group by COB_DATE, OPT_BUY_SELL, OPT_TYPE, STRIKE, INDEX_FAMILY, INDEX, CR_OPTION_EXPIRY_DATE, MONEYNESS_BUCKET, REFERENCE_INDEX_ENTITY_NAME, OPTION_EXPIRY_BUCKET
having sum(PV_MIN50) != 0 or sum(PV_MIN10) != 0 or sum(PV_PLS10) != 0 or sum(PV_PLS50) != 0 or sum(PV_PLS100) != 0 or sum(PV_PLS200) != 0 or sum(NOTIONAL) != 0 or sum(EXPOSURE) != 0 or sum(CR_GAMMA) != 0