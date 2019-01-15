Select COB_DATE, BOOK, PRODUCT_TYPE_CODE, CONSOLIDATED_RATING, CREDIT_SPREAD_BUCKET,
Case when GICS_LEVEL_1_NAME in ('FINANCIALS','REAL ESTATE') then 'Fins'
when GICS_LEVEL_1_NAME in ('ENERGY', 'MATERIALS', 'UTILITIES') Then 'Energy' 
when GICS_LEVEL_1_NAME in ('TELECOMMUNICATION SERVICES', 'INFORMATION TECHNOLOGY') Then 'IT' 
Else GICS_LEVEL_1_NAME END  as Industry,
POSITION_ULT_ISSUER_PARTY_DARWIN_NAME as Name,
CASE WHEN PRODUCT_TYPE_CODE IN ('BANKDEBT') THEN 'LOAN'
WHEN PRODUCT_TYPE_CODE IN ('BOND', 'CASH', 'EQUITY', 'FEE', 'FLOOR', 'FX', 'GVTBOND', 'PREF'
        , 'TRS - BOND', 'TRS - GVTBOND', 'TRD_CLAIM', 'BOND ETF', 'BONDFUT', 'BONDIL', 'CONVRT', 'CLNBOND','MUNI') THEN 'CASH' 
WHEN PRODUCT_TYPE_CODE IN ('CRDINDEX', 'LOANINDEX', 'MUNICDX', 'CDSOPTIDX') THEN 'INDEX'
ELSE 'CDS / OTHER SYNTHETIC' END AS SECTYPE2,
Case When a.PRODUCT_TYPE_CODE='BANKDEBT' then 'Loan' else 'Hedge' end as LOAN_TYPE,
Case when a.FACILITY_TYPE = 'N/A' Then PRODUCT_TYPE_CODE
when a.FACILITY_TYPE in ('LOC', 'LETTER OF CREDIT/STANDBY (TERM)','LETTER OF CREDIT/STANDBY (REVOLVING)','RBL/LOC RCF FOR RESERVED BASED LENDING') Then 'LOC'
when a.FACILITY_TYPE like '%RCF%' Then 'Revolver'
when a.FACILITY_TYPE like 'REVOLVER%' Then 'Revolver'
when a.FACILITY_TYPE like 'TERM%' Then 'Term Loan'
ELSE a.FACILITY_TYPE END as Sectype,
Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - NIG WORKOUT HFI MSBNA-LIWMB'))Then 'EventRel HFI'
when BOOK like 'HFI%' Then 'HFI' Else 'Other' END as Level6,

sum(CR_SPREAD_MARK_5Y * abs(USD_PV10_BENCH)) as SPREAD,
sum(abs(USD_PV10_BENCH)) as sum_ABS_PV10,
ABS(sum(USD_PV10_BENCH)) ABS_PV10,
sum(USD_PV10_BENCH) pv10,
Case when PRODUCT_TYPE_CODE in ('CDSOPTIDX', 'CRDINDEX') Then sum(coalesce(SLIDE_PVSPRCOMP_PLS_50PCT_USD ,0)) ELSE sum(coalesce(a.SLIDE_PV_PLS_50PCT_USD,0)) END as PV50,
sum(coalesce(a.USD_EXPOSURE,0)) NET_EXPOSURE,
sum(coalesce(a.USD_NOTIONAL,0)) NOTIONAL
from cdwuser.U_EXP_MSR a
where
    a.COB_DATE IN 
    ('2018-02-28')
and (CCC_BUSINESS_AREA = 'LENDING' or (CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' and CCC_PRODUCT_LINE = 'PRIMARY - LOANS'))
and CCC_STRATEGY not in ('CORPORATE LOAN STRATEGY','PROJECT FINANCE','EVENT - INV GRADE','EVENT - NON INV GRADE')
and a.VERTICAL_SYSTEM not like 'PIPELINE%'

Group by COB_DATE, BOOK, PRODUCT_TYPE_CODE, CONSOLIDATED_RATING, CREDIT_SPREAD_BUCKET,
Case when GICS_LEVEL_1_NAME in ('FINANCIALS','REAL ESTATE') then 'Fins'
when GICS_LEVEL_1_NAME in ('ENERGY', 'MATERIALS', 'UTILITIES') Then 'Energy' 
when GICS_LEVEL_1_NAME in ('TELECOMMUNICATION SERVICES', 'INFORMATION TECHNOLOGY') Then 'IT' 
Else GICS_LEVEL_1_NAME END,
POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
CASE WHEN PRODUCT_TYPE_CODE IN ('BANKDEBT') THEN 'LOAN'
WHEN PRODUCT_TYPE_CODE IN ('BOND', 'CASH', 'EQUITY', 'FEE', 'FLOOR', 'FX', 'GVTBOND', 'PREF'
        , 'TRS - BOND', 'TRS - GVTBOND', 'TRD_CLAIM', 'BOND ETF', 'BONDFUT', 'BONDIL', 'CONVRT', 'CLNBOND','MUNI') THEN 'CASH' 
WHEN PRODUCT_TYPE_CODE IN ('CRDINDEX', 'LOANINDEX', 'MUNICDX', 'CDSOPTIDX') THEN 'INDEX'
ELSE 'CDS / OTHER SYNTHETIC' END,
Case When a.PRODUCT_TYPE_CODE='BANKDEBT' then 'Loan' else 'Hedge' end,
Case when a.FACILITY_TYPE = 'N/A' Then PRODUCT_TYPE_CODE
when a.FACILITY_TYPE in ('LOC', 'LETTER OF CREDIT/STANDBY (TERM)','LETTER OF CREDIT/STANDBY (REVOLVING)','RBL/LOC RCF FOR RESERVED BASED LENDING') Then 'LOC'
when a.FACILITY_TYPE like '%RCF%' Then 'Revolver'
when a.FACILITY_TYPE like 'REVOLVER%' Then 'Revolver'
when a.FACILITY_TYPE like 'TERM%' Then 'Term Loan'
ELSE a.FACILITY_TYPE END,
Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - NIG WORKOUT HFI MSBNA-LIWMB'))Then 'EventRel HFI'
when BOOK like 'HFI%' Then 'HFI' Else 'Other' END