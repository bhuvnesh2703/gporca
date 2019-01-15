SELECT
    TAPSCUSIP,
    POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
    VINTAGE,
    INSURER_RATING,
    SUM (CASE WHEN COB_DATE = '2018-02-28' then USD_EXPOSURE else 0 end) AS NET_EXPOSURE,
    ABS(SUM (CASE WHEN COB_DATE = '2018-02-28' then USD_EXPOSURE else 0 end)) AS ABS_NET_EXPOSURE,
    SUM (CASE WHEN COB_DATE = '2018-02-28' then USD_EXPOSURE else -USD_EXPOSURE end) AS NET_EXPOSURE_DOD
FROM cdwuser.U_CR_MSR a
WHERE
    a.COB_DATE IN 
    ('2018-02-28', '2018-02-27')
    AND a.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP'    
    AND NOT a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'WAREHOUSE')
    AND NOT (a.SPG_DESC = 'CMBS INDEX' AND a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'SPG MANAGEMENT'))
    AND CCC_PL_REPORTING_REGION = 'AMERICAS'
    AND SPG_DESC in ('CMBS IO REREMIC', 'CMBS SECURITY REREMIC')
GROUP BY
    TAPSCUSIP,
    POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
    VINTAGE,
    INSURER_RATING