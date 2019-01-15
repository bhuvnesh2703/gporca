Select
COB_DATE,
Case When ARB_ADJ_INDEX_SHORT_NAME != 'UNDEFINED' Then 'Index'
When PRODUCT_TYPE_CODE in ('BOND ETF','TRS BOND ETF','TRS LOAN ETF') Then 'Index'
When PRODUCT_TYPE_CODE in ('CRDBSKT','CLNBSKT','CLNCLN','CLNBOND','CLNCDO2','CRDINDEX','CLNZCS','CLNDEFSWAP') Then 'Bespoke'
When PRODUCT_TYPE_CODE = 'DEFSWAP' Then 'CDS'
Else 'Other' End as PRODUCT_TYPE_CODE,
CASE WHEN CCC_STRATEGY = 'INDEX PROD TRADING - NA1' then 'NA INDEX TRADING'
          WHEN CCC_STRATEGY = 'EUROPEAN CREDIT FLOW1' then 'EU INDEX TRADING'
          WHEN CCC_STRATEGY = 'US INDEX TRANCHE' then 'NA INDEX TRANCHE'
          WHEN CCC_STRATEGY = 'TRS TRADING NA' then 'NA TRS INDEX TRADING'
          WHEN CCC_STRATEGY = 'OPTIONS' then 'EU INDEX OPTIONS'
          else CCC_STRATEGY end as CCC_STRATEGY,
case WHEN BOOK = 'ABCHN' Then 'DSP INDEX PRODUCTS'
        WHEN CCC_STRATEGY in ('LEGACY MUNI DERIVATIVES','LEGACY BESPOKE CDOS', 'LEGACY FLOW CDS', 'LEGACY HF DERIVS EU',  'LEGACY PORTFOLIO TRADING CDP', 'LEGACY TRANCHED INDEX EU', 'MANAGEMENT OTHER CDP') or CCC_PRODUCT_LINE = 'DSP LEGACY' Then 'DSP Legacy'
        WHEN (CCC_PRODUCT_LINE = 'DSP TRS FINANCING' or CCC_STRATEGY = 'DSP TRS FINANCING1') Then 'DSP TRS Financing'
        WHEN CCC_PRODUCT_LINE = 'DSP EXOTICS' or CCC_STRATEGY in ('EXOTICS CDP', 'FULL CAP BESPOKE') Then 'DSP Exotics'
        else 'DSP INDEX PRODUCTS' end as PRODUCT_LINE, 
sum(a.USD_PV10_BENCH) PV10,
sum(a.USD_PV01SPRD) PV01,
sum(CASE WHEN a.PRODUCT_HIERARCHY_LEVEL7='CR_LOANETF' then USD_MARKET_VALUE ELSE USD_EXPOSURE END) as NE,
sum(a.USD_IR_UNIFIED_PV01) ir01,
sum(a.USD_IRPV01SPRD)*100 as IRPV01,
sum(a.USD_IR_KAPPA) ir_kappa,
sum(a.USD_IR_PARTIAL_GAMMA) ir_gamma
from cdwuser.U_EXP_MSR a
where
    A.COB_DATE in ('2018-02-28','2018-02-27')
    AND (CCC_BUSINESS_AREA = 'DSP - CREDIT' or
    CCC_PRODUCT_LINE = 'INDEX PROD TRADING - NA' or 
     a.CCC_COST_CENTER_CODE = 'Q476') 
Group by
COB_DATE,
Case When ARB_ADJ_INDEX_SHORT_NAME != 'UNDEFINED' Then 'Index'
When PRODUCT_TYPE_CODE in ('BOND ETF','TRS BOND ETF','TRS LOAN ETF') Then 'Index'
When PRODUCT_TYPE_CODE in ('CRDBSKT','CLNBSKT','CLNCLN','CLNBOND','CLNCDO2','CRDINDEX','CLNZCS','CLNDEFSWAP') Then 'Bespoke'
When PRODUCT_TYPE_CODE = 'DEFSWAP' Then 'CDS'
Else 'Other' End,
CASE WHEN CCC_STRATEGY = 'INDEX PROD TRADING - NA1' then 'NA INDEX TRADING'
          WHEN CCC_STRATEGY = 'EUROPEAN CREDIT FLOW1' then 'EU INDEX TRADING'
          WHEN CCC_STRATEGY = 'US INDEX TRANCHE' then 'NA INDEX TRANCHE'
          WHEN CCC_STRATEGY = 'TRS TRADING NA' then 'NA TRS INDEX TRADING'
          WHEN CCC_STRATEGY = 'OPTIONS' then 'EU INDEX OPTIONS'
          else CCC_STRATEGY end,
case WHEN BOOK = 'ABCHN' Then 'DSP INDEX PRODUCTS'
        WHEN CCC_STRATEGY in ('LEGACY MUNI DERIVATIVES','LEGACY BESPOKE CDOS', 'LEGACY FLOW CDS', 'LEGACY HF DERIVS EU',  'LEGACY PORTFOLIO TRADING CDP', 'LEGACY TRANCHED INDEX EU', 'MANAGEMENT OTHER CDP') or CCC_PRODUCT_LINE = 'DSP LEGACY' Then 'DSP Legacy'
        WHEN (CCC_PRODUCT_LINE = 'DSP TRS FINANCING' or CCC_STRATEGY = 'DSP TRS FINANCING1') Then 'DSP TRS Financing'
        WHEN CCC_PRODUCT_LINE = 'DSP EXOTICS' or CCC_STRATEGY in ('EXOTICS CDP', 'FULL CAP BESPOKE') Then 'DSP Exotics'
        else 'DSP INDEX PRODUCTS' end