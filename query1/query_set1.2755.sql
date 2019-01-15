Select COB_DATE, 
Case when GICS_LEVEL_1_NAME in ('FINANCIALS','REAL ESTATE') then 'Fins'
when GICS_LEVEL_1_NAME in ('ENERGY', 'MATERIALS', 'UTILITIES') Then 'Energy' 
when GICS_LEVEL_1_NAME in ('TELECOMMUNICATION SERVICES', 'INFORMATION TECHNOLOGY') Then 'IT' 
Else GICS_LEVEL_1_NAME END  as Industry,
Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - PCIF ELTH WORKOUT HFI MSBNA-LPFWB'))Then 'EventRel HFI'
when BOOK like 'HFI%' Then 'HFI' Else 'Other' END as Level6,
ABS(sum(coalesce(USD_PV10_BENCH,0))) ABS_PV10,
sum(coalesce(USD_PV10_BENCH,0)) pv10

from cdwuser.U_CR_MSR a
where
    a.COB_DATE IN 
    ('2018-02-28','2018-02-27')
and (CCC_BUSINESS_AREA = 'LENDING' or (CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' and CCC_PRODUCT_LINE = 'PRIMARY - LOANS'))
and CCC_STRATEGY not in ('CORPORATE LOAN STRATEGY','PROJECT FINANCE','EVENT - INV GRADE','EVENT - NON INV GRADE')
and a.VERTICAL_SYSTEM not like 'PIPELINE%'
AND CCC_TAPS_COMPANY in ('1633')
Group by COB_DATE, 
Case when GICS_LEVEL_1_NAME in ('FINANCIALS','REAL ESTATE') then 'Fins'
when GICS_LEVEL_1_NAME in ('ENERGY', 'MATERIALS', 'UTILITIES') Then 'Energy' 
when GICS_LEVEL_1_NAME in ('TELECOMMUNICATION SERVICES', 'INFORMATION TECHNOLOGY') Then 'IT' 
Else GICS_LEVEL_1_NAME END,
Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - PCIF ELTH WORKOUT HFI MSBNA-LPFWB'))Then 'EventRel HFI'
when BOOK like 'HFI%' Then 'HFI' Else 'Other' END