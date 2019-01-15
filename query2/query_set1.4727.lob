SELECT
    A.COB_DATE,
    A.CONSOLIDATED_RATING,
    Case When a.PRODUCT_TYPE_CODE in ('BANKDEBT') then 'Loan' else 'Hedge' end as LOAN_TYPE,
    Case when GICS_LEVEL_1_NAME in ('FINANCIALS','REAL ESTATE') then 'Fins'
    when GICS_LEVEL_1_NAME in ('ENERGY', 'MATERIALS', 'UTILITIES') Then 'Energy' 
    when GICS_LEVEL_1_NAME in ('TELECOMMUNICATION SERVICES', 'INFORMATION TECHNOLOGY') Then 'IT' 
    Else GICS_LEVEL_1_NAME END as Industry,
    Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable'
    when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable'
    when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG', 'PMGNE')) Then 'HFS Hedgeable'
    when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging'
    when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS'
    when BOOK like '%ELTH FVO%' Then 'EventRel FVO'
    when BOOK in ('REL - NA - SPG UTAH','PMGPB') Then 'Relationship Legacy'
    when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - NIG WORKOUT HFI MSBNA-LIWMB'))Then 'EventRel HFI'
    when BOOK like 'HFI%' Then 'HFI' Else 'Other' END as Level6,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME as Name,
    sum(USD_PV10_BENCH) pv10,
    ABS(sum(USD_PV10_BENCH)) ABS_PV10,
    sum(coalesce(a.USD_EXPOSURE,0)) NET_EXPOSURE,
    sum(coalesce(a.USD_NOTIONAL,0)) NOTIONAL
FROM cdwuser.U_EXP_MSR a
where a.COB_DATE IN 
    ('2018-02-28')
    and (CCC_BUSINESS_AREA = 'LENDING' or (CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' and CCC_PRODUCT_LINE = 'PRIMARY - LOANS'))
    and CCC_STRATEGY not in ('CORPORATE LOAN STRATEGY','PROJECT FINANCE','EVENT - INV GRADE','EVENT - NON INV GRADE')
    and a.VERTICAL_SYSTEM not like 'PIPELINE%'
    and A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME NOT IN (SELECT
                                                        DISTINCT
                                                        b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
                                                        FROM cdwuser.U_CR_MSR b
                                                    WHERE
                                                        b.COB_DATE IN ('2018-01-31')
                                                        and (CCC_BUSINESS_AREA = 'LENDING' or (CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' and CCC_PRODUCT_LINE = 'PRIMARY - LOANS'))
                                                        and CCC_STRATEGY not in ('CORPORATE LOAN STRATEGY','PROJECT FINANCE','EVENT - INV GRADE','EVENT - NON INV GRADE')
                                                        and b.VERTICAL_SYSTEM not like 'PIPELINE%')

GROUP BY
    a.COB_DATE,
    A.CONSOLIDATED_RATING,
    Case When a.PRODUCT_TYPE_CODE in ('BANKDEBT') then 'Loan' else 'Hedge' end,
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
    when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - NIG WORKOUT HFI MSBNA-LIWMB'))Then 'EventRel HFI'
    when BOOK like 'HFI%' Then 'HFI' Else 'Other' END,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
HAVING ABS(SUM(A.USD_PV10_BENCH)) >1