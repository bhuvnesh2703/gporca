SELECT
    a.COB_DATE,
    SUM (CASE WHEN a.PRODUCT_TYPE_CODE IN ('ADR', 'STOCK', 'SWAP', 'WARRNT', 'ETF', 'OPTION') THEN a.USD_DELTA ELSE a.USD_NET_EXPOSURE END) AS NET_EXPOSURE
FROM cdwuser.U_DM_CC A
WHERE
    a.COB_DATE IN (
'2018-01-17', 
'2018-01-24', 
'2018-01-31', 
'2018-02-07', 
'2018-02-14', 
'2018-02-21', 
'2018-02-28'
) AND

    a.CCC_PL_REPORTING_REGION in ('EMEA','EUROPE') AND 
a.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND
    PRODUCT_TYPE_CODE NOT IN ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX','FUTURE','BOND ETF')
GROUP BY
    a.COB_DATE