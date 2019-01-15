Select COB_DATE, BOOK, PRODUCT_TYPE_CODE,
Case When a.PRODUCT_TYPE_CODE='BANKDEBT' then 'Loan' else 'Hedge' end as LOAN_TYPE,
a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME as Name,
Case when a.FACILITY_TYPE = 'N/A' Then PRODUCT_TYPE_CODE
when a.FACILITY_TYPE in ('LOC', 'LETTER OF CREDIT/STANDBY (TERM)','LETTER OF CREDIT/STANDBY (REVOLVING)','RBL/LOC RCF FOR RESERVED BASED LENDING') Then 'LOC'
when a.FACILITY_TYPE like '%RCF%' Then 'Revolver'
when a.FACILITY_TYPE like 'REVOLVER%' Then 'Revolver'
ELSE 'Term Loan' END as Sectype,
Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - PCIF ELTH WORKOUT HFI MSBNA-LPFWB'))Then 'EventRel HFI'
when BOOK like 'HFI%' Then 'HFI' Else 'Other' END as Level6,
sum(coalesce(USD_PV01SPRD,0)) pv01,
sum(coalesce(USD_PV10_BENCH,0)) pv10,
Case when PRODUCT_TYPE_CODE in ('CDSOPTIDX', 'CRDINDEX') Then sum(coalesce(SLIDE_PVSPRCOMP_PLS_50PCT_USD ,0)) ELSE sum(coalesce(a.SLIDE_PV_PLS_50PCT_USD,0)) END as PV50,
Case when PRODUCT_TYPE_CODE in ('CDSOPTIDX', 'CRDINDEX') Then sum(coalesce(SLIDE_PVSPRCOMP_PLS_100PCT_USD ,0)) ELSE sum(coalesce(a.SLIDE_PV_PLS_100PCT_USD,0)) END as PV100,
Case when PRODUCT_TYPE_CODE in ('CDSOPTIDX', 'CRDINDEX') Then sum(coalesce(SLIDE_PVSPRCOMP_PLS_200PCT_USD ,0)) ELSE sum(coalesce(a.SLIDE_PV_PLS_200PCT_USD,0)) END as PV200,
sum(coalesce(a.USD_EXPOSURE,0)) NET_EXPOSURE,
sum(coalesce(a.USD_NOTIONAL,0)) NOTIONAL
from cdwuser.U_EXP_MSR a
where
    a.COB_DATE IN 
    ('2018-02-28','2018-01-31','2017-12-29','2017-09-29','2017-06-30')
and (CCC_BUSINESS_AREA = 'LENDING' or (CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' and CCC_PRODUCT_LINE = 'PRIMARY - LOANS'))
and CCC_STRATEGY not in ('CORPORATE LOAN STRATEGY','PROJECT FINANCE','EVENT - INV GRADE','EVENT - NON INV GRADE')
and a.VERTICAL_SYSTEM not like 'PIPELINE%'

Group by COB_DATE, BOOK, PRODUCT_TYPE_CODE,
Case When a.PRODUCT_TYPE_CODE='BANKDEBT' then 'Loan' else 'Hedge' end,
a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
Case when a.FACILITY_TYPE = 'N/A' Then PRODUCT_TYPE_CODE
when a.FACILITY_TYPE in ('LOC', 'LETTER OF CREDIT/STANDBY (TERM)','LETTER OF CREDIT/STANDBY (REVOLVING)','RBL/LOC RCF FOR RESERVED BASED LENDING') Then 'LOC'
when a.FACILITY_TYPE like '%RCF%' Then 'Revolver'
when a.FACILITY_TYPE like 'REVOLVER%' Then 'Revolver'
ELSE 'Term Loan' END,
Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - PCIF ELTH WORKOUT HFI MSBNA-LPFWB'))Then 'EventRel HFI'
when BOOK like 'HFI%' Then 'HFI' Else 'Other' END