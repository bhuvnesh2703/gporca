Select COB_DATE, 
Case When CCC_STRATEGY in ('NON CORE WORKOUT') Then 'PROP WORKOUT' 
When CCC_PRODUCT_LINE = 'GLOBAL STRUCT PRODUCTS' Then 'GLOBAL STRUCT PRODUCTS' 
When CCC_PRODUCT_LINE = 'MSPI INVESTING' Then 'MSPI INVESTING' 
When CCC_PRODUCT_LINE = 'PROP WORKOUT' Then 'PROP WORKOUT' 
When CCC_PRODUCT_LINE = 'RESIDENTIAL' Then 'RESIDENTIAL' 
When CCC_PRODUCT_LINE = 'COMMERCIAL RE (PTG)' Then 'COMMERCIAL RE (PTG)' 
Else CCC_BUSINESS_AREA end as CCC_BUSINESS_AREA, 
CCC_PRODUCT_LINE, CCC_STRATEGY, BOOK, CCC_BANKING_TRADING, PRODUCT_TYPE_CODE, a.FID1_INDUSTRY_NAME_LEVEL2 
, PRODUCT_TYPE_CODE, CURRENCY_OF_MEASURE, a.COUNTRY_CD_OF_RISK, MRD_RATING 
, PRODUCT_DESCRIPTION, a.POSITION_ISSUER_PARTY_DARWIN_NAME, a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, SPG_DESC 
, Case When PRODUCT_TYPE_CODE in ('BANKDEBT','BOND','DEFSWAP','FRN') THEN 'Credit' When PRODUCT_TYPE_CODE in ('ADR','STOCK','SWAP','WARRNT','ETF','OPTION','FUTURE','EQUITY') Then 'Equity' 
Else 'Other' END as PRODUCT_TYPE 
, sum(case when a.PRODUCT_TYPE_CODE in ('ADR','STOCK','SWAP','WARRNT','ETF','OPTION','FUTURE') then Coalesce(USD_DELTA,0) else Coalesce(USD_EXPOSURE,0) end) as INVENTORY 
, sum(Coalesce(USD_EXPOSURE,0)) as NET_EXPOSURE 
, sum(Coalesce(USD_DELTA,0)) as DELTA 
, sum(Coalesce(USD_IR_UNIFIED_PV01,0)) IR_PV01 
, sum(Coalesce(USD_PV01SPRD,0)) CS01 
, sum(Coalesce(USD_PV10_BENCH,0)) PV10 
, sum(Coalesce(USD_FX,0)) FX 
from cdwuser.U_EXP_MSR a 
where COB_DATE in ('2018-02-28','2018-01-31') 
and (CCC_BUSINESS_AREA in ('GLOBAL STRUCT PRODUCTS','MSPI INVESTING','PROP WORKOUT','RESIDENTIAL','COMMERCIAL RE (PTG)') 
or (CCC_BUSINESS_AREA = 'NON CORE' and CCC_PRODUCT_LINE in ('GLOBAL STRUCT PRODUCTS','MSPI INVESTING','PROP WORKOUT','RESIDENTIAL','COMMERCIAL RE (PTG)'))) 
Group by COB_DATE, 
Case When CCC_STRATEGY in ('NON CORE WORKOUT') Then 'PROP WORKOUT' 
When CCC_PRODUCT_LINE = 'GLOBAL STRUCT PRODUCTS' Then 'GLOBAL STRUCT PRODUCTS' 
When CCC_PRODUCT_LINE = 'MSPI INVESTING' Then 'MSPI INVESTING' 
When CCC_PRODUCT_LINE = 'PROP WORKOUT' Then 'PROP WORKOUT' 
When CCC_PRODUCT_LINE = 'RESIDENTIAL' Then 'RESIDENTIAL' 
When CCC_PRODUCT_LINE = 'COMMERCIAL RE (PTG)' Then 'COMMERCIAL RE (PTG)' 
Else CCC_BUSINESS_AREA end, 
CCC_PRODUCT_LINE, CCC_STRATEGY, BOOK, CCC_BANKING_TRADING, PRODUCT_TYPE_CODE, a.FID1_INDUSTRY_NAME_LEVEL2 
, PRODUCT_TYPE_CODE, CURRENCY_OF_MEASURE, a.COUNTRY_CD_OF_RISK, MRD_RATING 
, PRODUCT_DESCRIPTION, a.POSITION_ISSUER_PARTY_DARWIN_NAME, a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, SPG_DESC 
, Case When PRODUCT_TYPE_CODE in ('BANKDEBT','BOND','DEFSWAP','FRN') THEN 'Credit' When PRODUCT_TYPE_CODE in ('ADR','STOCK','SWAP','WARRNT','ETF','OPTION','FUTURE','EQUITY') Then 'Equity' 
Else 'Other' END