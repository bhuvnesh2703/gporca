select Rank() Over(Order by Abs(sum(Coalesce(D30,0))) Desc) as ORDER_RANK, 
b.CCC_PRODUCT_LINE,  
b.PRODUCT_DESCRIPTION,
b.UNDERLIER_TICK, 
b.OPTION_TYPE, 
b.STRIKE, 
b.OPTION_MONEYNESS, 
b.EXPIRATION_DATE
, sum(CONTRACT_AMT) as CONTRACT_AMT
, sum(Coalesce(D30,0))/1000 as D30
, sum(Coalesce(D20,0))/1000 as D20
, sum(Coalesce(D10,0))/1000 as D10
, sum(Coalesce(D05,0))/1000 as D05
, sum(Coalesce(P05,0))/1000 as P05
, sum(Coalesce(P10,0))/1000 as P10
, sum(DELTA) as DELTA
from
    (Select a.COB_DATE, 
a.PROCESS_ID, 
a.POSITION_ID, 
a.CCC_PRODUCT_LINE,
a.UNDERLIER_TICK,
a.STRIKE, 
a.OPTION_MONEYNESS, 
a.EXPIRATION_DATE, 
PRODUCT_DESCRIPTION,
Case substr(PRODUCT_DESCRIPTION,length(PRODUCT_DESCRIPTION)+1-2) when 'AP' Then 'American Put' 
When 'EP' Then 'European Put' 
When 'AC' Then 'American Call' 
When 'EC' Then 'European Call' End as OPTION_TYPE
    , Max(TRADE_SIZE) as CONTRACT_AMT
    , sum(a.USD_DELTA)/1000 as DELTA
    , sum(a.USD_EQ_GAMMA)/1000 GAMMA
    , sum(a.USD_EQ_KAPPA)/1000 as EQ_VEGA
    , sum(a.SLIDE_EQ_MIN_05_USD) D05
    , sum(a.SLIDE_EQ_PLS_05_USD) P05
    , sum(a.SLIDE_EQ_PLS_10_USD) P10
    , sum(Coalesce(SLIDE_EQ_MIN_05_USD,0) - Coalesce(USD_DELTA,0) * -.05) DELTA_ADJ_05
    , sum(Coalesce(SLIDE_EQ_MIN_10_USD,0)) D10
    , sum(Coalesce(SLIDE_EQ_MIN_10_USD,0) - Coalesce(USD_DELTA,0) * -.1) DELTA_ADJ_10
    , sum(Coalesce(SLIDE_EQ_MIN_20_USD,0)) D20
    , sum(Coalesce(SLIDE_EQ_MIN_20_USD,0) - Coalesce(USD_DELTA,0) * -.2) DELTA_ADJ_20
    , sum(Coalesce(SLIDE_EQ_MIN_30_USD,0)) D30
    , sum(Coalesce(SLIDE_EQ_MIN_30_USD,0) - Coalesce(USD_DELTA,0) * -.3) DELTA_ADJ_30
    from cdwuser.U_EQ_MSR a
WHERE
    a.COB_DATE IN 
('2018-02-28') 
    and CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP' 
    and PRODUCT_TYPE_CODE = 'OPTION'
    group by a.COB_DATE, 
a.PROCESS_ID, 
a.POSITION_ID, 
a.CCC_PRODUCT_LINE, 
a.UNDERLIER_TICK, 
a.STRIKE, 
a.OPTION_MONEYNESS, 
a.EXPIRATION_DATE, 
PRODUCT_DESCRIPTION,
Case substr(PRODUCT_DESCRIPTION,length(PRODUCT_DESCRIPTION)+1-2) when 'AP' Then 'American Put' When 'EP' Then 'European Put' When 'AC' Then 'American Call' When 'EC' Then 'European Call' End
 ) b
group by b.COB_DATE, b.CCC_PRODUCT_LINE, b.PRODUCT_DESCRIPTION, b.OPTION_TYPE, b.UNDERLIER_TICK, b.STRIKE, b.OPTION_MONEYNESS, b.EXPIRATION_DATE
having sum(Coalesce(DELTA,0)) + sum(Coalesce(D05,0)) + sum(Coalesce(D10,0)) + sum(Coalesce(D20,0)) + sum(Coalesce(D30,0)) != 0