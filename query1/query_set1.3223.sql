SELECT
    COB_DATE,
    CASE WHEN SPG_DESC in ('RMBS ALTA DEFAULT SWAP', 'RMBS CDO DEFAULT SWAP', 'RMBS DEFAULT SWAP', 'RMBS PRIME DEFAULT SWAP','RMBS PRIME DEFAULT SWAP', 'RMBS NON CONFORMING DEFAULT SWAP', 'CMBS DEFAULT SWAP') THEN 'CDS' 
    WHEN SPG_DESC IN ('RMBS INDEX TRANCHE', 'RMBS SUB PRIME INDEX', 'RMBS PRIME INDEX', 'CMBS INDEX') THEN 'Index'
    ELSE 'CASH' END AS CASH_SYNTHETIC,
    CASE WHEN a.POSITION_ISSUER_PARTY_DARWIN_NAME in ('TESCO PLC','TESCO PROPERTY FINANCE 6 PLC','LAKESIDE ASSET MANAGEMENT', 'AEOLOS S.A', 'MORTGAGE FUNDING 2008-1 PLC','FASTNET SECURITIES 9 LIMITED', 'BERICA PMI S.R.L.','MARCHE MUTUI SRL','MARCHE MUTUI 4 S.R.L.','DELAMARE FINANCE PLC','DRAGON FINANCE B.V.') THEN 'HIGHER' 
    WHEN SPG_DESC = 'CMBS MEZZANINE LOAN' THEN 'CMBS LOAN'
    WHEN SPG_DESC = 'RMBS SD LOAN' THEN 'NPL LOAN' 
    WHEN SPG_DESC = 'RMBS PRIME LOAN' THEN 'PRIME LOAN'
    WHEN SPG_PRODUCT_TYPE_GROUP IN ('CORPORATE', 'RESI- NON AGENCY', 'SUBPRIME') AND SPG_PRODUCT_TYPE NOT IN ('CORPORATE SINGLE NAME','CORPORATE SINGLE NAME INDEX','RESI- NON AGENCY ALT A REREMIC','RESI- NON AGENCY PRIME LOAN','RESI- NON AGENCY PRIME REREMIC','RESI- NON AGENCY SCRATCH & DENT','SUBPRIME CDO','SUBPRIME REREMIC') AND INSURER_RATING NOT IN ('AAA','PENAAA','AM','AS') THEN 'LOWER' 
    WHEN INSURER_RATING IN ('AAA','PENAAA','AM', 'AS') AND SPG_DESC like 'CMBS%' THEN 'HIGHER' 
    WHEN INSURER_RATING NOT IN ('AAA','PENAAA','AM', 'AS') AND SPG_DESC like 'CMBS%' THEN 'LOWER' 
    WHEN INSURER_RATING NOT IN ('AAA','PENAAA','AM', 'AS') AND SPG_PRODUCT_TYPE_GROUP = 'ABS' AND DETACHMENT < 1 THEN 'LOWER' 
    WHEN SPG_PRODUCT_TYPE IN ('CMBS CDO', 'SUBPRIME CDO') THEN 'LOWER' 
    WHEN SPG_PRODUCT_TYPE_GROUP = 'CMBS' AND SPG_PRODUCT_TYPE = 'CMBS LOAN' AND BOOK = 'CMBS_SECONDARY' THEN 'LOWER' 
    WHEN SPG_DESC IN ('RMBS CDO') THEN 'LOWER' else 'HIGHER' 
    END AS GROUPED_RATING,
    SUM (a.USD_EXPOSURE) AS NET_EXPOSURE
FROM cdwuser.U_CR_MSR a
WHERE
    a.COB_DATE IN 
    ('2018-02-28', '2018-01-31')
    AND a.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP'    
    AND NOT a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'WAREHOUSE')
    AND NOT (a.SPG_DESC = 'CMBS INDEX' AND a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'SPG MANAGEMENT'))
    AND CCC_PL_REPORTING_REGION = 'EMEA'
    AND spg_desc like '%CMBS%' and spg_desc not in ('CMBS LOAN', 'CMBS LOAN IO')
GROUP BY
    COB_DATE,
    CASE WHEN SPG_DESC in ('RMBS ALTA DEFAULT SWAP', 'RMBS CDO DEFAULT SWAP', 'RMBS DEFAULT SWAP', 'RMBS PRIME DEFAULT SWAP','RMBS PRIME DEFAULT SWAP', 'RMBS NON CONFORMING DEFAULT SWAP', 'CMBS DEFAULT SWAP') THEN 'CDS' 
    WHEN SPG_DESC IN ('RMBS INDEX TRANCHE', 'RMBS SUB PRIME INDEX', 'RMBS PRIME INDEX', 'CMBS INDEX') THEN 'Index'
    ELSE 'CASH' END,
    CASE WHEN a.POSITION_ISSUER_PARTY_DARWIN_NAME in ('TESCO PLC','TESCO PROPERTY FINANCE 6 PLC','LAKESIDE ASSET MANAGEMENT', 'AEOLOS S.A', 'MORTGAGE FUNDING 2008-1 PLC','FASTNET SECURITIES 9 LIMITED', 'BERICA PMI S.R.L.','MARCHE MUTUI SRL','MARCHE MUTUI 4 S.R.L.','DELAMARE FINANCE PLC','DRAGON FINANCE B.V.') THEN 'HIGHER' 
    WHEN SPG_DESC = 'CMBS MEZZANINE LOAN' THEN 'CMBS LOAN'
    WHEN SPG_DESC = 'RMBS SD LOAN' THEN 'NPL LOAN' 
    WHEN SPG_DESC = 'RMBS PRIME LOAN' THEN 'PRIME LOAN'
    WHEN SPG_PRODUCT_TYPE_GROUP IN ('CORPORATE', 'RESI- NON AGENCY', 'SUBPRIME') AND SPG_PRODUCT_TYPE NOT IN ('CORPORATE SINGLE NAME','CORPORATE SINGLE NAME INDEX','RESI- NON AGENCY ALT A REREMIC','RESI- NON AGENCY PRIME LOAN','RESI- NON AGENCY PRIME REREMIC','RESI- NON AGENCY SCRATCH & DENT','SUBPRIME CDO','SUBPRIME REREMIC') AND INSURER_RATING NOT IN ('AAA','PENAAA','AM','AS') THEN 'LOWER' 
    WHEN INSURER_RATING IN ('AAA','PENAAA','AM', 'AS') AND SPG_DESC like 'CMBS%' THEN 'HIGHER' 
    WHEN INSURER_RATING NOT IN ('AAA','PENAAA','AM', 'AS') AND SPG_DESC like 'CMBS%' THEN 'LOWER' 
    WHEN INSURER_RATING NOT IN ('AAA','PENAAA','AM', 'AS') AND SPG_PRODUCT_TYPE_GROUP = 'ABS' AND DETACHMENT < 1 THEN 'LOWER' 
    WHEN SPG_PRODUCT_TYPE IN ('CMBS CDO', 'SUBPRIME CDO') THEN 'LOWER' 
    WHEN SPG_PRODUCT_TYPE_GROUP = 'CMBS' AND SPG_PRODUCT_TYPE = 'CMBS LOAN' AND BOOK = 'CMBS_SECONDARY' THEN 'LOWER' 
    WHEN SPG_DESC IN ('RMBS CDO') THEN 'LOWER' else 'HIGHER' 
    END