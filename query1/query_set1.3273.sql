SELECT
    a.COB_DATE,
    a.RATING2,
    a.SPG_DESC,
 CASE WHEN a.SPG_DESC IN ('CORPORATE INDEX', 'CORPORATE DEFAULT SWAP') AND a.CCC_PRODUCT_LINE NOT IN ('CRE LENDING', 'CREL BANK HFI', 'CRE LENDING SEC/HFS') THEN 'Corporates' 
    WHEN a.CCC_PRODUCT_LINE NOT IN ('AGENCY MBS', 'AGENCY TRADING') AND (a.SPG_DESC = 'GOVERNMENT' OR PRODUCT_TYPE_CODE in ('BERMUDAN_SWAPTION_VA','BONDFUTOPT','MISC'))  THEN 'IR Hedges - Treasury' 
    WHEN a.CCC_PRODUCT_LINE NOT IN ('AGENCY MBS', 'AGENCY TRADING') AND (spg_desc like 'RATE %' or spg_desc in ('SWAP','SWAPS')) THEN 'IR Hedges - Swap'    
    WHEN a.SPG_DESC = 'CMBS INDEX' AND a.CCC_PRODUCT_LINE IN ('SPG MANAGEMENT') THEN 'CMBX Hedge'
    ELSE 'EQUITY' END AS GROUPED_PRODUCT_TYPE,
CASE WHEN a.SPG_DESC IN ('RMBS AGENCY CMO', 'RMBS AGENCY CMO SUPPORT', 'RMBS AGENCY FLOATER', 'RMBS AGENCY INVERSE FLOATER') THEN 'CMO'
    WHEN a.SPG_DESC IN ('RMBS AGENCY IO', 'RMBS AGENCY PO', 'RMBS AGENCY IIO', 'RMBS IOS INDEX', 'RMBS MBX INDEX') THEN 'IO/PO/MBX' 
    WHEN a.SPG_DESC IN ('RMBS AGENCY ARM SECURITIES', 'RMBS MBS FREDDIE MAC POOL ARM', 'RMBS MBS FANNIE MAE POOL ARM', 'RMBS MBS GINNIE MAE POOL ARM', 'RMBS MBS FREDDIE MAC TBA ARM', 'RMBS MBS FANNIE MAE TBA ARM', 'RMBS MBS GINNIE MAE TBA ARM', 'RMBS MBS FANNIE MAE SECURITY', 'RMBS MBS FREDDIE MAC SECURITY', 'RMBS MBS GINNIE MAE SECURITY', 'RMBS MBS FANNIE MAE POOL', 'RMBS MBS FREDDIE MAC POOL', 'RMBS MBS GINNIE MAE POOL', 'RMBS MBS FREDDIE MAC POOL FIX', 'RMBS MBS FANNIE MAE POOL FIX', 'RMBS MBS GINNIE MAE POOL FIX', 'OTHER RMBS') THEN 'Pool' 
    WHEN a.SPG_DESC IN ('RMBS MBS FANNIE MAE TBA', 'RMBS MBS FREDDIE MAC TBA', 'RMBS MBS GINNIE MAE TBA', 'RMBS MBS FREDDIE MAC TBA FIX','RMBS MBS FANNIE MAE TBA FIX', 'RMBS MBS GINNIE MAE TBA FIX') and CCC_STRATEGY ='AGENCY CMBS' THEN 'TBA CMBS'
    WHEN a.SPG_DESC IN ('RMBS MBS FANNIE MAE TBA', 'RMBS MBS FREDDIE MAC TBA', 'RMBS MBS GINNIE MAE TBA', 'RMBS MBS FREDDIE MAC TBA FIX','RMBS MBS FANNIE MAE TBA FIX', 'RMBS MBS GINNIE MAE TBA FIX') THEN 'TBA' 
    WHEN a.SPG_DESC IN ('AGENCY CMBS SECURITY') THEN 'Agency CMBS' 
    WHEN (SPG_DESC IN ('GOVERNMENT', 'RATE FUTURES', 'SWAP', 'SWAPS') OR (SPG_DESC = 'OTHER' AND ACCOUNT IN ('07500JR18'))) AND CCC_STRATEGY IN ('AGENCY CMBS') THEN 'CMBS Swap & Govt'
    WHEN (SPG_DESC IN ('GOVERNMENT', 'RATE FUTURES', 'SWAP', 'SWAPS') OR (SPG_DESC = 'OTHER' AND ACCOUNT IN ('07500JR18'))) AND CCC_STRATEGY IN ('AGENCY DEBT TRADING','AGENCY WM TRADING','WM AGENCY TRADING','WM RATES TRADING') THEN 'Debenture Swap & Govt'
    WHEN (SPG_DESC IN ('GOVERNMENT', 'RATE FUTURES', 'SWAP', 'SWAPS') OR (SPG_DESC = 'OTHER' AND ACCOUNT IN ('07500JR18'))) AND CCC_STRATEGY IN ('AGENCY MORTGAGE TRADING1', 'AGENCY CMO TRADING', 'AGENCY MBS TRADING', 'AGENCY CMO', 'AGENCY PASS-THROUGH', 'US AGENCIES') THEN 'Resi Swap & Govt'
    WHEN (SPG_DESC = 'OTHER' AND PRODUCT_TYPE_CODE = 'AGN') OR (SPG_DESC= 'CORPORATE AND CDS' AND ACCOUNT IN ('07200MT46','072001118','07200MTF1','07200MT38','07200KS58','07200MP40')) THEN 'Agency Debentures'
    WHEN spg_desc in ('ABS STUDENT SECURITY') THEN 'ABS Agency' 
    WHEN a.SPG_DESC = 'AGENCY CMBS IO' THEN 'Agency CMBS IO' ELSE 'OTHER' END AS GROUPED_AGENCIES,
    SUM (a.USD_PV01SPRD) AS PV01SPRD,
    SUM (a.USD_CONVX) AS CONVX,
    SUM (a.USD_IR_KAPPA) AS KAPPA,
    SUM (a.USD_EXPOSURE) AS NET_EXPOSURE,
    SUM (a.USD_IR_UNIFIED_PV01) AS PV01,
    SUM (a.USD_PV10_BENCH_COMP) AS PV10,
    SUM (a.USD_DELTA) AS DELTA
FROM cdwuser.U_DM_SPG a
WHERE
    a.COB_DATE IN 
    ('2018-02-28', '2018-01-31', '2017-12-29', '2017-09-29', '2017-06-30')
    AND a.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP'
    AND NOT a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'WAREHOUSE')
GROUP BY
    a.COB_DATE,
    a.RATING2,
    a.SPG_DESC,
 CASE WHEN a.SPG_DESC IN ('CORPORATE INDEX', 'CORPORATE DEFAULT SWAP') AND a.CCC_PRODUCT_LINE NOT IN ('CRE LENDING', 'CREL BANK HFI', 'CRE LENDING SEC/HFS') THEN 'Corporates' 
    WHEN a.CCC_PRODUCT_LINE NOT IN ('AGENCY MBS', 'AGENCY TRADING') AND (a.SPG_DESC = 'GOVERNMENT' OR PRODUCT_TYPE_CODE in ('BERMUDAN_SWAPTION_VA','BONDFUTOPT','MISC'))  THEN 'IR Hedges - Treasury' 
    WHEN a.CCC_PRODUCT_LINE NOT IN ('AGENCY MBS', 'AGENCY TRADING') AND (spg_desc like 'RATE %' or spg_desc in ('SWAP','SWAPS')) THEN 'IR Hedges - Swap'    
    WHEN a.SPG_DESC = 'CMBS INDEX' AND a.CCC_PRODUCT_LINE IN ('SPG MANAGEMENT') THEN 'CMBX Hedge'
    ELSE 'EQUITY' END,
CASE WHEN a.SPG_DESC IN ('RMBS AGENCY CMO', 'RMBS AGENCY CMO SUPPORT', 'RMBS AGENCY FLOATER', 'RMBS AGENCY INVERSE FLOATER') THEN 'CMO'
    WHEN a.SPG_DESC IN ('RMBS AGENCY IO', 'RMBS AGENCY PO', 'RMBS AGENCY IIO', 'RMBS IOS INDEX', 'RMBS MBX INDEX') THEN 'IO/PO/MBX' 
    WHEN a.SPG_DESC IN ('RMBS AGENCY ARM SECURITIES', 'RMBS MBS FREDDIE MAC POOL ARM', 'RMBS MBS FANNIE MAE POOL ARM', 'RMBS MBS GINNIE MAE POOL ARM', 'RMBS MBS FREDDIE MAC TBA ARM', 'RMBS MBS FANNIE MAE TBA ARM', 'RMBS MBS GINNIE MAE TBA ARM', 'RMBS MBS FANNIE MAE SECURITY', 'RMBS MBS FREDDIE MAC SECURITY', 'RMBS MBS GINNIE MAE SECURITY', 'RMBS MBS FANNIE MAE POOL', 'RMBS MBS FREDDIE MAC POOL', 'RMBS MBS GINNIE MAE POOL', 'RMBS MBS FREDDIE MAC POOL FIX', 'RMBS MBS FANNIE MAE POOL FIX', 'RMBS MBS GINNIE MAE POOL FIX', 'OTHER RMBS') THEN 'Pool' 
    WHEN a.SPG_DESC IN ('RMBS MBS FANNIE MAE TBA', 'RMBS MBS FREDDIE MAC TBA', 'RMBS MBS GINNIE MAE TBA', 'RMBS MBS FREDDIE MAC TBA FIX','RMBS MBS FANNIE MAE TBA FIX', 'RMBS MBS GINNIE MAE TBA FIX') and CCC_STRATEGY ='AGENCY CMBS' THEN 'TBA CMBS'
    WHEN a.SPG_DESC IN ('RMBS MBS FANNIE MAE TBA', 'RMBS MBS FREDDIE MAC TBA', 'RMBS MBS GINNIE MAE TBA', 'RMBS MBS FREDDIE MAC TBA FIX','RMBS MBS FANNIE MAE TBA FIX', 'RMBS MBS GINNIE MAE TBA FIX') THEN 'TBA' 
    WHEN a.SPG_DESC IN ('AGENCY CMBS SECURITY') THEN 'Agency CMBS' 
    WHEN (SPG_DESC IN ('GOVERNMENT', 'RATE FUTURES', 'SWAP', 'SWAPS') OR (SPG_DESC = 'OTHER' AND ACCOUNT IN ('07500JR18'))) AND CCC_STRATEGY IN ('AGENCY CMBS') THEN 'CMBS Swap & Govt'
    WHEN (SPG_DESC IN ('GOVERNMENT', 'RATE FUTURES', 'SWAP', 'SWAPS') OR (SPG_DESC = 'OTHER' AND ACCOUNT IN ('07500JR18'))) AND CCC_STRATEGY IN ('AGENCY DEBT TRADING','AGENCY WM TRADING','WM AGENCY TRADING','WM RATES TRADING') THEN 'Debenture Swap & Govt'
    WHEN (SPG_DESC IN ('GOVERNMENT', 'RATE FUTURES', 'SWAP', 'SWAPS') OR (SPG_DESC = 'OTHER' AND ACCOUNT IN ('07500JR18'))) AND CCC_STRATEGY IN ('AGENCY MORTGAGE TRADING1', 'AGENCY CMO TRADING', 'AGENCY MBS TRADING', 'AGENCY CMO', 'AGENCY PASS-THROUGH', 'US AGENCIES') THEN 'Resi Swap & Govt'
    WHEN (SPG_DESC = 'OTHER' AND PRODUCT_TYPE_CODE = 'AGN') OR (SPG_DESC= 'CORPORATE AND CDS' AND ACCOUNT IN ('07200MT46','072001118','07200MTF1','07200MT38','07200KS58','07200MP40')) THEN 'Agency Debentures'
    WHEN spg_desc in ('ABS STUDENT SECURITY') THEN 'ABS Agency' 
    WHEN a.SPG_DESC = 'AGENCY CMBS IO' THEN 'Agency CMBS IO' ELSE 'OTHER' END