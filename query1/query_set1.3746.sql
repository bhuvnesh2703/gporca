SELECT
    COB_DATE,
    SPG_DESC, a.CCC_BANKING_TRADING,
    a.LE_GROUP,    A.CCC_BUSINESS_AREA, CCC_DIVISION,
    CCC_PL_REPORTING_REGION,
    a.VERTICAL_SYSTEM,CCC_PRODUCT_LINE,
    a.PRODUCT_TYPE_CODE,
    a.PRODUCT_TYPE_NAME,
    a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
    CASE WHEN INSURER_RATING = 'AAA' THEN 'AAA'
    ELSE 'MEZZ' END AS RATING,
    CASE WHEN (product_description LIKE 'GRMI%' OR
               product_description LIKE 'GRNM%' OR
               product_description LIKE 'GRAN%') THEN 'GRAN' WHEN (product_description LIKE 'AVLM%' OR
                                                                   product_description LIKE 'AIRE%' OR
                                                                   product_description LIKE 'PARMTG%' OR
                                                                   product_description LIKE 'PARAGON%') THEN 'BUYT' WHEN
    product_type_code IN ('RMBS_ARM', 'RMBS_FIX', 'RMBS', 'FIX') AND
    product_sub_type_code LIKE 'PRIME%' AND
    issuer_country_code = 'GBP'
    THEN 'UKPR' WHEN
    product_type_code IN ('RMBS_ARM', 'RMBS_FIX', 'RMBS', 'FIX') AND
    product_sub_type_code LIKE 'PRIME%' AND
    issuer_country_code IN ('ITA', 'ESP')
    THEN 'PERI1' WHEN
    product_type_code IN ('RMBS_ARM', 'RMBS_FIX', 'RMBS', 'FIX') AND
    product_sub_type_code LIKE 'PRIME%' AND
    issuer_country_code IN ('IRL', 'PRT', 'GRC')
    THEN 'PERI2' WHEN
    product_type_code IN ('RMBS_ARM', 'RMBS_FIX', 'RMBS', 'FIX') AND
    product_sub_type_code LIKE 'PRIME%' AND
    issuer_country_code NOT IN ('ITA', 'ESP', 'IRL', 'PRT', 'GRC')
    THEN 'DUTC' WHEN ((product_type_code IN ('CDO_CASH') AND
                       product_sub_type_code IN ('CDO_ABS')) OR
 product_type_code LIKE 'RMBS%') THEN 'NONC' WHEN product_type_code = 'CMBS' THEN 'CMBS' WHEN ((product_type_code 
        IN ('CDO_CASH') 
          AND
    product_sub_type_code 
  NOT IN (
                                                                                                                                     'CDO_ABS'
              )) OR
                                                                                                                    product_type_code = 
                                                                                                                        'CLO') THEN 
        'CLO' WHEN (product_description LIKE 'PUNCH%' OR
                    product_description LIKE 'ENTERPRISE INNS%' OR
                    product_description LIKE 'UNIQUE PUB%' OR
                    product_description LIKE 'SPIRIT ISSUER%') THEN 'UKPU' WHEN product_type_code IN ('ABS') THEN 'SME'
    ELSE 'N/A' END as DESC,




    SUM (USD_EXPOSURE) AS NEXPOSURE


FROM cdwuser.U_CR_MSR a
WHERE
    cob_date IN ('2018-02-28','2018-02-21') 
  AND ( ccc_business_area IN ('SECURITIZED PRODUCTS GRP') OR  CCC_DIVISION = 'NON CORE' ) 
    AND a.COUNTRY_CD_OF_RISK in ('ESP') 
   AND ccc_product_line not like '%CRE%' and ccc_product_line not like '%WAREHOUSE%'
and a.PRODUCT_TYPE_CODE not in ('CRDINDEX','BOND')
    AND CCC_DIVISION NOT IN ('FID DVA', 'FIC DVA')  AND CCC_STRATEGY NOT IN ('MS DVA STR NOTES IED') 
GROUP BY
    COB_DATE,CCC_DIVISION,
    CASE WHEN INSURER_RATING = 'AAA' THEN 'AAA'
    ELSE 'MEZZ' END,
    ccc_gl_company_code,
    a.LE_GROUP,
    SPG_DESC,
    CCC_PL_REPORTING_REGION,
    a.VERTICAL_SYSTEM,    A.CCC_BUSINESS_AREA,
    PRODUCT_TYPE_NAME,a.CCC_BANKING_TRADING,
    a.PRODUCT_TYPE_CODE,CCC_PRODUCT_LINE,
    POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
    CASE WHEN (product_description LIKE 'GRMI%' OR
               product_description LIKE 'GRNM%' OR
               product_description LIKE 'GRAN%') THEN 'GRAN' WHEN (product_description LIKE 'AVLM%' OR
                                                                   product_description LIKE 'AIRE%' OR
                                                                   product_description LIKE 'PARMTG%' OR
                                                                   product_description LIKE 'PARAGON%') THEN 'BUYT' WHEN
    product_type_code IN ('RMBS_ARM', 'RMBS_FIX', 'RMBS', 'FIX') AND
    product_sub_type_code LIKE 'PRIME%' AND
    issuer_country_code = 'GBP'
    THEN 'UKPR' WHEN
    product_type_code IN ('RMBS_ARM', 'RMBS_FIX', 'RMBS', 'FIX') AND
    product_sub_type_code LIKE 'PRIME%' AND
    issuer_country_code IN ('ITA', 'ESP')
    THEN 'PERI1' WHEN
    product_type_code IN ('RMBS_ARM', 'RMBS_FIX', 'RMBS', 'FIX') AND
    product_sub_type_code LIKE 'PRIME%' AND
    issuer_country_code IN ('IRL', 'PRT', 'GRC')
    THEN 'PERI2' WHEN
    product_type_code IN ('RMBS_ARM', 'RMBS_FIX', 'RMBS', 'FIX') AND
    product_sub_type_code LIKE 'PRIME%' AND
    issuer_country_code NOT IN ('ITA', 'ESP', 'IRL', 'PRT', 'GRC')
    THEN 'DUTC' WHEN ((product_type_code IN ('CDO_CASH') AND
                       product_sub_type_code IN ('CDO_ABS')) OR
                      product_type_code LIKE 'RMBS%') THEN 'NONC' WHEN product_type_code = 'CMBS' THEN 'CMBS' WHEN ((product_type_code 
                                                                                                                         IN ('CDO_CASH') 
                                                                                                                         AND
                                                                                                                     product_sub_type_code 
                                                                                                                         NOT IN (
                                                                                                                                     'CDO_ABS'
                                                                                                                         )) OR
                                                                                                                    product_type_code = 
                                                                                                                        'CLO') THEN 
        'CLO' WHEN (product_description LIKE 'PUNCH%' OR
                    product_description LIKE 'ENTERPRISE INNS%' OR
                    product_description LIKE 'UNIQUE PUB%' OR
                    product_description LIKE 'SPIRIT ISSUER%') THEN 'UKPU' WHEN product_type_code IN ('ABS') THEN 'SME'
    ELSE 'N/A' END