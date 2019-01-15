SELECT
    A.COB_DATE,
CASE WHEN PRODUCT_HIERARCHY_LEVEL7 LIKE '%TRANCHED INDEX%' THEN (CASE WHEN PRODUCT_HIERARCHY_LEVEL7 LIKE '%OFF THE RUN%' THEN 
                                                                         'TRANCHED_INDEX_OFFTR'
                                                                     ELSE 'TRANCHED_INDEX_OTR' END) WHEN PRODUCT_TYPE_CODE IN (
                                                                                                                                   'BANKDEBT'
        ) THEN 'BANKDEBT' WHEN PRODUCT_TYPE_CODE IN ('MUNI', 'MUNI_TAXABLE', 'BOND', 'CP', 'FRN', 'LOANHYLD', 'PREF', 'TRD_CLAIM', 
                                                         'GVTBOND', 'CLNCLN', 'EQUITY', 'BONDFUT', 'TRS - BOND', 'TRS - GVTBOND', 
                                                         'BOND ETF', 'BONDIL', 'CONVRT', 'CLNBOND') THEN 'CASH' WHEN PRODUCT_TYPE_CODE 
        IN ('SPRDOPT', 'CDSOPTIDX') THEN 'OPTION' WHEN PRODUCT_TYPE_CODE IN ('CANCDFS', 'DEFSWAP', 'FEE', 'LOANCDS', 'TRRSWAP', 
                                                                                 'MUNICDS', 'CLNDEFSWAP') THEN 'CDS' WHEN 
        PRODUCT_TYPE_CODE IN ('CRDCLN', 'LOC', 'NTDCDO', 'ZCS') THEN 'EXOTIC' WHEN PRODUCT_TYPE_CODE IN ('CRDINDEX', 'LOANINDEX', 
                                                                                                             'MUNICDX') THEN (CASE WHEN 
                                                                                                                                  FID1_INDEX_FAMILY 
                                                                                                                                  LIKE 
                                                                                                                                  '%CDX%' 
                                                                                                                                  THEN 
                                                                                                                                  'INDEX-US' 
                                                                                                                                  WHEN 
                                                                                                                                  FID1_INDEX_FAMILY 
                                                                                                                                  LIKE 
                                                                                                                                  '%ITRAXX%' 
                                                                                                                                  THEN 
                                                                                                                                  'INDEX-EU'
                                                                                                                              ELSE 
                                                                                                                                  'INDEX-OTHER' 
                                                                                                                                  END) 
        WHEN PRODUCT_TYPE_CODE IN ('CLNBSKT', 'CRDBSKT') THEN 'BESPOKE BASKET' WHEN PRODUCT_TYPE_CODE IN ('CLNCDO2', 'CDO2') THEN 'CDO2'
    ELSE 'UNDEFINED' END AS PRODUCT_TYPE_CODE_GROUPED, 
SUM (CASE WHEN a.PRODUCT_TYPE_CODE IN ('ADR', 'STOCK', 'SWAP', 'WARRNT', 'ETF', 'OPTION') THEN a.USD_DELTA ELSE a.USD_EXPOSURE END) AS NET_EXPOSURE
FROM cdwuser.U_EXP_MSR a
WHERE
    A.COB_DATE IN ('2018-02-28') AND 
            a.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND
            PRODUCT_TYPE_CODE NOT IN ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX','FUTURE','BOND ETF')
GROUP BY
    COB_DATE,
CASE WHEN PRODUCT_HIERARCHY_LEVEL7 LIKE '%TRANCHED INDEX%' THEN (CASE WHEN PRODUCT_HIERARCHY_LEVEL7 LIKE '%OFF THE RUN%' THEN 
                                                                         'TRANCHED_INDEX_OFFTR'
                                                                     ELSE 'TRANCHED_INDEX_OTR' END) WHEN PRODUCT_TYPE_CODE IN (
                                                                                                                                   'BANKDEBT'
        ) THEN 'BANKDEBT' WHEN PRODUCT_TYPE_CODE IN ('MUNI', 'MUNI_TAXABLE', 'BOND', 'CP', 'FRN', 'LOANHYLD', 'PREF', 'TRD_CLAIM', 
                                                         'GVTBOND', 'CLNCLN', 'EQUITY', 'BONDFUT', 'TRS - BOND', 'TRS - GVTBOND', 
                                                         'BOND ETF', 'BONDIL', 'CONVRT', 'CLNBOND') THEN 'CASH' WHEN PRODUCT_TYPE_CODE 
        IN ('SPRDOPT', 'CDSOPTIDX') THEN 'OPTION' WHEN PRODUCT_TYPE_CODE IN ('CANCDFS', 'DEFSWAP', 'FEE', 'LOANCDS', 'TRRSWAP', 
                                                                                 'MUNICDS', 'CLNDEFSWAP') THEN 'CDS' WHEN 
        PRODUCT_TYPE_CODE IN ('CRDCLN', 'LOC', 'NTDCDO', 'ZCS') THEN 'EXOTIC' WHEN PRODUCT_TYPE_CODE IN ('CRDINDEX', 'LOANINDEX', 
                                                                                                             'MUNICDX') THEN (CASE WHEN 
                                                                                                                                  FID1_INDEX_FAMILY 
                                                                                                                                  LIKE 
                                                                                                                                  '%CDX%' 
                                                                                                                                  THEN 
                                                                                                                                  'INDEX-US' 
                                                                                                                                  WHEN 
                                                                                                                                  FID1_INDEX_FAMILY 
                                                                                                                                  LIKE 
                                                                                                                                  '%ITRAXX%' 
                                                                                                                                  THEN 
                                                                                                                                  'INDEX-EU'
                                                                                                                              ELSE 
                                                                                                                                  'INDEX-OTHER' 
                                                                                                                                  END) 
        WHEN PRODUCT_TYPE_CODE IN ('CLNBSKT', 'CRDBSKT') THEN 'BESPOKE BASKET' WHEN PRODUCT_TYPE_CODE IN ('CLNCDO2', 'CDO2') THEN 'CDO2'
    ELSE 'UNDEFINED' END