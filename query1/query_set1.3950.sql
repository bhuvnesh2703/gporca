SELECT
    A.DISTRESSED_SECURITY_DESCRIPTION,
    A.NET_EXPOSURE,
    A.NET_EXPOSURE - COALESCE(B.NET_EXPOSURE,0) AS DIFF
FROM
    (
        SELECT
            A.COB_DATE,
    a.DISTRESSED_SECURITY_DESCRIPTION, 
    SUM (CASE WHEN a.PRODUCT_TYPE_CODE IN ('ADR', 'STOCK', 'SWAP', 'WARRNT', 'ETF', 'OPTION') THEN a.USD_DELTA ELSE a.USD_NET_EXPOSURE END) AS NET_EXPOSURE
FROM cdwuser.U_DM_CC a
        WHERE
            A.COB_DATE IN ('2018-02-28') AND 


a.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND
    PRODUCT_TYPE_CODE NOT IN ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX','FUTURE','BOND ETF')
GROUP BY
    a.COB_DATE, 
    a.DISTRESSED_SECURITY_DESCRIPTION 
HAVING SUM (CASE WHEN a.PRODUCT_TYPE_CODE IN ('ADR', 'STOCK', 'SWAP', 'WARRNT', 'ETF', 'OPTION') THEN a.USD_DELTA ELSE a.USD_NET_EXPOSURE END) IS NOT NULL
        ORDER BY SUM (CASE WHEN a.PRODUCT_TYPE_CODE IN ('ADR', 'STOCK', 'SWAP', 'WARRNT', 'ETF', 'OPTION') THEN a.USD_DELTA ELSE a.USD_NET_EXPOSURE END) DESC
        FETCH FIRST 5 ROWS ONLY
    )
    A
    LEFT OUTER JOIN
    (
        SELECT
            A.COB_DATE,
    a.DISTRESSED_SECURITY_DESCRIPTION, 
    SUM (CASE WHEN a.PRODUCT_TYPE_CODE IN ('ADR', 'STOCK', 'SWAP', 'WARRNT', 'ETF', 'OPTION') THEN a.USD_DELTA ELSE a.USD_NET_EXPOSURE END) AS NET_EXPOSURE
FROM cdwuser.U_DM_CC a
        WHERE
             A.COB_DATE IN ('2018-02-27') AND 


a.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND
    PRODUCT_TYPE_CODE NOT IN ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX','FUTURE','BOND ETF')
        GROUP BY
            COB_DATE,
    a.DISTRESSED_SECURITY_DESCRIPTION
    )
    B
    ON A.DISTRESSED_SECURITY_DESCRIPTION = B.DISTRESSED_SECURITY_DESCRIPTION