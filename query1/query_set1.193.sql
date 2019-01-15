SELECT
    COALESCE (M.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, N.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME) AS POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, 
    M.USD_NET_EXPOSURE AS CURR_NE,
    M.USD_PV01SPRD AS CURR_SPV01,
    N.USD_NET_EXPOSURE AS PRV_NE,
    N.USD_PV01SPRD AS PRV_SPV01,
     COALESCE (M.USD_NET_EXPOSURE,0) -  COALESCE (N.USD_NET_EXPOSURE,0) AS NE_DIFF,
    ABS ( COALESCE (M.USD_NET_EXPOSURE,0) -  COALESCE (N.USD_NET_EXPOSURE,0)) AS ABS_NE_DIFF,
     COALESCE (M.USD_PV01SPRD,0) -  COALESCE (N.USD_PV01SPRD,0) AS SPV01_DIFF,
    ABS ( COALESCE (M.USD_PV01SPRD,0) -  COALESCE (N.USD_PV01SPRD,0)) AS ABS_SPV01_DIFF
FROM
    (
        SELECT
            CUR.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
            COALESCE (SUM (CUR.USD_NET_EXPOSURE),0) AS USD_NET_EXPOSURE,
            COALESCE (SUM (CUR.USD_PV01SPRD),0) AS USD_PV01SPRD
        FROM
            (
                SELECT
                    a.CCC_STRATEGY,
                    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
                    A.MUNI_TYPE_CODE,
                    a.COB_DATE,
                    COALESCE (SUM (A.USD_NET_EXPOSURE),0) AS USD_NET_EXPOSURE,
                    COALESCE (SUM (A.USD_PV01SPRD),0) AS USD_PV01SPRD
                FROM CDWUSER.U_DM_CC A
                WHERE
            A.COB_DATE IN ('2018-02-28') AND 
                    A.MUNI_TYPE_CODE IS NOT NULL AND CCC_BANKING_TRADING = 'TRADING' AND 
                    ((a.CCC_BUSINESS_AREA = 'MUNICIPAL SECURITIES' AND
                      a.PRODUCT_TYPE_CODE IN ('BONDFUT', 'RATEFUT', 'GVTBOND', 'MUNI', 'MUNI_TAXABLE', 'SWAPTION', 'SWAP', 'BOND', 'BONDFUTOPT')
                         )) AND
 A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME IN ('CALIFORNIA, STATE OF', 
'CITY OF CHICAGO', 'NEW JERSEY, STATE OF', 
                                                            'TEXAS, STATE OF', 'CITY OF NEW YORK', 'NEW YORK, STATE OF', 
                                                            'MARYLAND, STATE OF', 'ILLINOIS, STATE OF', 
                                                            'COMMONWEALTH OF PUERTO RICO')

                GROUP BY
                    a.MUNI_TYPE_CODE,
                    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
                    a.COB_DATE,
                    a.CCC_STRATEGY
            )
            CUR
        WHERE CUR.MUNI_TYPE_CODE IN ('MUNI', 'MUNI_TAXABLE')
        GROUP BY CUR.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    )
    M
   FULL OUTER JOIN
    (
        SELECT
            CUR.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
            COALESCE (SUM (CUR.USD_NET_EXPOSURE),0) AS USD_NET_EXPOSURE,
            COALESCE (SUM (CUR.USD_PV01SPRD),0) AS USD_PV01SPRD
        FROM
            (
                SELECT
                    a.CCC_STRATEGY,
                    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
                    A.MUNI_TYPE_CODE,
                    a.COB_DATE,
                    COALESCE (SUM (A.USD_NET_EXPOSURE),0) AS USD_NET_EXPOSURE,
                    COALESCE (SUM (A.USD_PV01SPRD),0) AS USD_PV01SPRD
                FROM CDWUSER.U_DM_CC A
                WHERE
            A.COB_DATE IN ('2018-02-27') AND 
                    A.MUNI_TYPE_CODE IS NOT NULL AND CCC_BANKING_TRADING = 'TRADING' AND 
                    ((a.CCC_BUSINESS_AREA = 'MUNICIPAL SECURITIES' AND
                      a.PRODUCT_TYPE_CODE IN ('BONDFUT', 'RATEFUT', 'GVTBOND', 'MUNI', 'MUNI_TAXABLE', 'SWAPTION', 'BOND', 'BONDFUTOPT')
                         )) AND
 A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME IN ('CALIFORNIA, STATE OF', 
'CITY OF CHICAGO', 'NEW JERSEY, STATE OF', 
                                                            'TEXAS, STATE OF', 'CITY OF NEW YORK', 'NEW YORK, STATE OF', 
                                                            'MARYLAND, STATE OF', 'ILLINOIS, STATE OF', 
                                                            'COMMONWEALTH OF PUERTO RICO')

                GROUP BY
                    a.MUNI_TYPE_CODE,
                    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
                    a.COB_DATE,
                    a.CCC_STRATEGY
            )
            CUR
        WHERE CUR.MUNI_TYPE_CODE IN ('MUNI', 'MUNI_TAXABLE')
        GROUP BY CUR.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
    )
    N
    ON M.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME = N.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME
