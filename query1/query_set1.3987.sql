SELECT
    CASE WHEN A.Product_Type_Code in ('MUNI_TAXABLE','BOND') then 'MUNI_TAXABLE'
    WHEN A.Product_Type_Code in ('MUNI','MMDRATELOCK') then 'MUNI'
    ELSE 'HEDGES' END as MUNI_TYPE_CODE,
    a.COB_DATE,
    SUM (A.USD_EXPOSURE) AS USD_NET_EXPOSURE,
    SUM (a.USD_IR_UNIFIED_PV01) AS USD_PV01, 
    SUM(A.USD_PV01SPRD) AS USD_PV01SPRD,
    SUM(A.USD_UNSCALED_PV01) AS PV01_UNSCALED
FROM cdwuser.U_EXP_MSR A
WHERE
    a.COB_DATE IN ('2018-02-28', '2018-02-27') AND 


    a.CCC_BUSINESS_AREA = 'MUNICIPAL SECURITIES' AND
    a.PRODUCT_TYPE_CODE IN ('BONDFUT', 'RATEFUT', 'GVTBOND', 'MUNI', 'MUNI_TAXABLE', 'SWAPTION', 'SWAP', 'BOND', 'BONDFUTOPT','MMDRATELOCK','RATEFUTOPT', 'BONDOPT')
GROUP BY
    CASE WHEN A.Product_Type_Code in ('MUNI_TAXABLE','BOND') then 'MUNI_TAXABLE'
    WHEN A.Product_Type_Code in ('MUNI','MMDRATELOCK') then 'MUNI'
    ELSE 'HEDGES' END,
    a.COB_DATE