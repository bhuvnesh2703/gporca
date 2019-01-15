SELECT
    a.COB_DATE,
    CASE WHEN CDP_TYPE='OTHER' and PRODUCT_TYPE_CODE = 'DEFSWAP' THEN 'CDS'
    WHEN CDP_TYPE='OTHER' and PRODUCT_TYPE_CODE='CRDINDEX' THEN 'Index'
    WHEN CDP_TYPE='OTHER' and PRODUCT_TYPE_CODE='CDSOPTIDX' THEN 'Index Option'
    ELSE CDP_TYPE end as CDP_TYPE,
    SUM (USD_PV10_BENCH) AS USD_PV10_BENCH,
    SUM (USD_PV01SPRD) AS USD_PV01SPRD,
    SUM (USD_SENSITIVITY_SCALED) AS CORR_01
FROM cdwuser.U_DM_CC A
WHERE
    a.COB_DATE IN ('2018-02-28', '2018-02-21') AND 


    ccc_business_area IN ('CREDIT DERIVATIVE PROD', 'DSP - CREDIT')
GROUP BY
    a.COB_DATE,
    CASE WHEN CDP_TYPE='OTHER' and PRODUCT_TYPE_CODE = 'DEFSWAP' THEN 'CDS'
    WHEN CDP_TYPE='OTHER' and PRODUCT_TYPE_CODE='CRDINDEX' THEN 'Index'
    WHEN CDP_TYPE='OTHER' and PRODUCT_TYPE_CODE='CDSOPTIDX' THEN 'Index Option'
    ELSE CDP_TYPE end