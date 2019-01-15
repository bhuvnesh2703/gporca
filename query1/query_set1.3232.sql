SELECT COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, CCC_STRATEGY, BOOK, 
TAPSCUSIP, VERTICAL_SYSTEM, CCAR_ASSET_PRODUCT_CATEGORY, CCC_BANKING_TRADING, 
A.SPG_DESC, A.PRODUCT_TYPE_CODE, A.PRODUCT_SUB_TYPE_CODE, VINTAGE, CCC_TAPS_COMPANY,
PAYOFF_MODEL, MARKET_MODEL, ACCOUNT, TRADE_TYPE, COUNTRY_CD_OF_RISK, DETACHMENT, TAPSCUSIP,
POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, DETACHMENT,CCC_PL_REPORTING_REGION, rating_group, bhc_rating_group, CCAR_BUSINESS_CATEGORY,       
A.USD_IR_KAPPA,
CASE 
        WHEN ACCOUNT IN ('071003909') THEN CCAR_ASSET_PRODUCT_CATEGORY||' ARMS'

        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: AGENCY DEBT/DEBENTURES', 'Agencies: NON-US AGENCY PRODUCTS') AND PAYOFF_MODEL LIKE '%CALLABLE%' AND (SPG_DESC LIKE '%FANNIE MAE%' OR  POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL NATIONAL MORTGAGE ASSOCIATION') THEN CCAR_ASSET_PRODUCT_CATEGORY||' CALLABLE FNMA'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: AGENCY DEBT/DEBENTURES', 'Agencies: NON-US AGENCY PRODUCTS') AND PAYOFF_MODEL LIKE '%CALLABLE%' AND (SPG_DESC LIKE '%FREDDIE MAC%' OR  POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL HOME LOAN MORTGAGE CORPORATION') THEN CCAR_ASSET_PRODUCT_CATEGORY||' CALLABLE FHLMC'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: AGENCY DEBT/DEBENTURES', 'Agencies: NON-US AGENCY PRODUCTS') AND PAYOFF_MODEL LIKE '%CALLABLE%' THEN CCAR_ASSET_PRODUCT_CATEGORY||' CALLABLE OTHERS'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: AGENCY DEBT/DEBENTURES', 'Agencies: NON-US AGENCY PRODUCTS') AND (SPG_DESC LIKE '%FANNIE MAE%' OR  POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL NATIONAL MORTGAGE ASSOCIATION') THEN CCAR_ASSET_PRODUCT_CATEGORY||' NC FNMA'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: AGENCY DEBT/DEBENTURES', 'Agencies: NON-US AGENCY PRODUCTS') AND (SPG_DESC LIKE '%FREDDIE MAC%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL HOME LOAN MORTGAGE CORPORATION') THEN CCAR_ASSET_PRODUCT_CATEGORY||' NC FHLMC'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: AGENCY DEBT/DEBENTURES', 'Agencies: NON-US AGENCY PRODUCTS') THEN CCAR_ASSET_PRODUCT_CATEGORY||' NC OTHERS' 
        
        -- Agency PT, by agency, deliverable, need to confirm GNMA logic
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: PASS-THROUGHS') AND (SPG_DESC LIKE '%FANNIE MAE%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL NATIONAL MORTGAGE ASSOCIATION') AND TRADE_TYPE = 'NON-DELIVERABLE' THEN CCAR_ASSET_PRODUCT_CATEGORY||' FNMA NON DELIVERABLE'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: PASS-THROUGHS') AND (SPG_DESC LIKE '%FANNIE MAE%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL NATIONAL MORTGAGE ASSOCIATION') THEN CCAR_ASSET_PRODUCT_CATEGORY||' FNMA DELIVERABLE'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: PASS-THROUGHS') AND (SPG_DESC LIKE '%FREDDIE MAC%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL HOME LOAN MORTGAGE CORPORATION') AND TRADE_TYPE = 'NON-DELIVERABLE' THEN CCAR_ASSET_PRODUCT_CATEGORY||' FHLMC NON DELIVERABLE'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: PASS-THROUGHS') AND (SPG_DESC LIKE '%FREDDIE MAC%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL HOME LOAN MORTGAGE CORPORATION') THEN CCAR_ASSET_PRODUCT_CATEGORY||' FHLMC DELIVERABLE'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: PASS-THROUGHS') AND (SPG_DESC LIKE '%GINNIE MAE%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'UNITED STATES OF AMERICA') AND TRADE_TYPE = 'NON-DELIVERABLE' THEN CCAR_ASSET_PRODUCT_CATEGORY||' GNMA NON DELIVERABLE'  
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: PASS-THROUGHS') AND (SPG_DESC LIKE '%GINNIE MAE%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'UNITED STATES OF AMERICA') THEN CCAR_ASSET_PRODUCT_CATEGORY||' GNMA DELIVERABLE'  
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: PASS-THROUGHS') AND TRADE_TYPE = 'NON-DELIVERABLE' THEN CCAR_ASSET_PRODUCT_CATEGORY||' OTHERS NON DELIVERABLE'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY IN ('Agencies: PASS-THROUGHS')  THEN CCAR_ASSET_PRODUCT_CATEGORY||' OTHERS DELIVERABLE'
        
        -- Agency, need to confirm GNMA logic
        WHEN CCAR_ASSET_PRODUCT_CATEGORY LIKE 'Agencies%' AND (SPG_DESC LIKE '%FANNIE MAE%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL NATIONAL MORTGAGE ASSOCIATION') THEN CCAR_ASSET_PRODUCT_CATEGORY||' FNMA'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY LIKE 'Agencies%' AND (SPG_DESC LIKE '%FREDDIE MAC%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'FEDERAL HOME LOAN MORTGAGE CORPORATION') THEN CCAR_ASSET_PRODUCT_CATEGORY||' FHLMC'
        WHEN CCAR_ASSET_PRODUCT_CATEGORY LIKE 'Agencies%' AND (SPG_DESC LIKE '%GINNIE MAE%' OR POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = 'UNITED STATES OF AMERICA') THEN CCAR_ASSET_PRODUCT_CATEGORY||' GNMA'  
        WHEN CCAR_ASSET_PRODUCT_CATEGORY LIKE 'Agencies%' THEN CCAR_ASSET_PRODUCT_CATEGORY||' OTHERS'
ELSE 'NEED REVIEW' END AS Agency_Category,expiration_date,

SUM(USD_IR_UNIFIED_PV01)*1000 AS PV01, SUM(A.USD_PV01SPRD)*1000 AS PV01SPRD, SUM(A.USD_NOTIONAL)*1000 AS NOTIONAL, SUM(A.USD_EXPOSURE)*1000 AS EXPOSURE
FROM cdwuser.U_EXP_MSR A
WHERE
    a.COB_DATE IN 
('2018-02-28', 
'2018-01-31', 
'2018-01-31', 
'2017-12-29', 
'2017-12-29') 
AND ( (CCAR_ASSET_PRODUCT_CATEGORY LIKE 'Agencies%')  )
and ccar_business_category not in ('NOT_INCLUDED','NOT_INCLUDED - PRIVATE BANK')
GROUP BY COB_DATE, CCC_DIVISION, CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, rating_group, bhc_rating_group, CCC_STRATEGY, BOOK, TAPSCUSIP, VERTICAL_SYSTEM, CCAR_ASSET_PRODUCT_CATEGORY, CCC_BANKING_TRADING, A.SPG_DESC, A.PRODUCT_TYPE_CODE, A.PRODUCT_SUB_TYPE_CODE, TAPSCUSIP, VINTAGE, CCC_TAPS_COMPANY,
PAYOFF_MODEL, MARKET_MODEL, ACCOUNT, TRADE_TYPE, COUNTRY_CD_OF_RISK, DETACHMENT, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, DETACHMENT, CCC_PL_REPORTING_REGION, INSURER_RATING, CCAR_BUSINESS_CATEGORY,expiration_date,
A.USD_IR_KAPPA