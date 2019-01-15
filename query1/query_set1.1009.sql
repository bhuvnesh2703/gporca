SELECT          a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE         , 'MS CDS' AS CVA_Type_Flag         , CASE             WHEN substr(a.CURVE_NAME,1,4) in ('cpeu','cpjp','cpna','cpnj','cprw') THEN 'GENERIC'             WHEN substr(a.CURVE_NAME,1,3) in ('ute') THEN 'GENERIC'             WHEN CURVE_NAME = 'comcr_wind' THEN 'GENERIC'             ELSE 'NON-GENERIC'         END AS GENERIC_Flag     , SUM(a.USD_PV01SPRD)/1000 AS PV1SPRD     FROM          cdwuser.U_CR_MSR a     WHERE 1=1 and (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and CCC_PL_REPORTING_REGION in ('EMEA') AND          (             a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD')              OR              a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')             )         AND NOT COALESCE(a.CURVE_TYPE,'') IN ('CPCR_CLEAR', 'CPCRFUND', 'MS_SECCPM', 'CPCR_MPEFUND')         AND NOT a.PRODUCT_SUB_TYPE_CODE = 'CMBX'         AND NOT a.VERTICAL_SYSTEM LIKE 'SPG%'         AND a.USD_PV01SPRD IS NOT NULL         AND a.CURVE_NAME = 'cpcrmne'     GROUP BY         a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE         , GENERIC_Flag UNION ALL     SELECT          a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE     , 'INDEX' AS CVA_Type_Flag     , CASE         WHEN substr(a.CURVE_NAME,1,4) in ('cpeu','cpjp','cpna','cpnj','cprw') THEN 'GENERIC'         WHEN substr(a.CURVE_NAME,1,3) in ('ute') THEN 'GENERIC'         WHEN CURVE_NAME = 'comcr_wind' THEN 'GENERIC'         ELSE 'NON-GENERIC'     END AS GENERIC_Flag     , SUM(a.USD_PV01SPRD)/1000 AS PV1SPRD     FROM          cdwuser.U_CR_MSR a     WHERE 1=1 and (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and CCC_PL_REPORTING_REGION in ('EMEA') AND          (             a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD')              OR              a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')             )         AND NOT COALESCE(a.CURVE_TYPE,'') IN ('CPCR_CLEAR', 'CPCRFUND', 'MS_SECCPM', 'CPCR_MPEFUND')         AND NOT a.PRODUCT_SUB_TYPE_CODE = 'CMBX'         AND NOT a.VERTICAL_SYSTEM LIKE 'SPG%'         AND a.USD_PV01SPRD IS NOT NULL         AND NOT a.CURVE_NAME = 'cpcrmne'         AND a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX', 'CRDBSKT', 'CRDINDEX', 'LOANINDEX', 'MUNICDX')     GROUP BY         a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE         , GENERIC_Flag UNION ALL     SELECT          a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE     , 'GENERIC' AS CVA_Type_Flag     , CASE         WHEN substr(a.CURVE_NAME,1,4) in ('cpeu','cpjp','cpna','cpnj','cprw') THEN 'GENERIC'         WHEN substr(a.CURVE_NAME,1,3) in ('ute') THEN 'GENERIC'         WHEN CURVE_NAME = 'comcr_wind' THEN 'GENERIC'         ELSE 'NON-GENERIC'     END AS GENERIC_Flag     , SUM(a.USD_PV01SPRD)/1000 AS PV1SPRD     FROM          cdwuser.U_CR_MSR a     WHERE 1=1 and (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and CCC_PL_REPORTING_REGION in ('EMEA') AND          (             a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD')              OR              a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')             )         AND NOT COALESCE(a.CURVE_TYPE,'') IN ('CPCR_CLEAR', 'CPCRFUND', 'MS_SECCPM', 'CPCR_MPEFUND')         AND NOT a.PRODUCT_SUB_TYPE_CODE = 'CMBX'         AND NOT a.VERTICAL_SYSTEM LIKE 'SPG%'         AND a.USD_PV01SPRD IS NOT NULL         AND NOT a.CURVE_NAME = 'cpcrmne'         AND NOT a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX', 'CRDBSKT', 'CRDINDEX', 'LOANINDEX', 'MUNICDX')         AND (                 substr(a.CURVE_NAME,1,4) in ('cpeu','cpjp','cpna','cpnj','cprw')                 or                 substr(a.CURVE_NAME,1,3) in ('ute')                 or                 CURVE_NAME = 'comcr_wind'                 )     GROUP BY         a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE         , GENERIC_Flag UNION ALL     SELECT          a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE     , 'CVA/SN' AS CVA_Type_Flag     , CASE         WHEN substr(a.CURVE_NAME,1,4) in ('cpeu','cpjp','cpna','cpnj','cprw') THEN 'GENERIC'         WHEN substr(a.CURVE_NAME,1,3) in ('ute') THEN 'GENERIC'         WHEN CURVE_NAME = 'comcr_wind' THEN 'GENERIC'         ELSE 'NON-GENERIC'     END AS GENERIC_Flag     , SUM(a.USD_PV01SPRD)/1000 AS PV1SPRD     FROM          cdwuser.U_CR_MSR a     WHERE 1=1 and (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and CCC_PL_REPORTING_REGION in ('EMEA') AND          (             a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD')              OR              a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')             )         AND NOT COALESCE(a.CURVE_TYPE,'') IN ('CPCR_CLEAR', 'CPCRFUND', 'MS_SECCPM', 'CPCR_MPEFUND')         AND NOT a.PRODUCT_SUB_TYPE_CODE = 'CMBX'         AND NOT a.VERTICAL_SYSTEM LIKE 'SPG%'         AND a.USD_PV01SPRD IS NOT NULL         AND NOT a.CURVE_NAME = 'cpcrmne'         AND NOT a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX', 'CRDBSKT', 'CRDINDEX', 'LOANINDEX', 'MUNICDX')         AND NOT substr(a.CURVE_NAME,1,4) IN ('cpeu','cpjp','cpna','cpnj','cprw')         AND NOT substr(a.CURVE_NAME,1,3) IN ('ute')         AND NOT CURVE_NAME = 'comcr_wind'         AND (a.VERTICAL_SYSTEM LIKE 'C1%' or a.VERTICAL_SYSTEM LIKE 'BANKLOANS%')     GROUP BY         a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE         , GENERIC_Flag UNION ALL     SELECT          a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE     , 'CVA/SN' AS CVA_Type_Flag     , CASE         WHEN substr(a.CURVE_NAME,1,4) in ('cpeu','cpjp','cpna','cpnj','cprw') THEN 'GENERIC'         WHEN substr(a.CURVE_NAME,1,3) in ('ute') THEN 'GENERIC'         WHEN CURVE_NAME = 'comcr_wind' THEN 'GENERIC'         ELSE 'NON-GENERIC'     END AS GENERIC_Flag     , SUM(a.USD_PV01SPRD)/1000 AS PV1SPRD     FROM          cdwuser.U_CR_MSR a     WHERE 1=1 and (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and CCC_PL_REPORTING_REGION in ('EMEA') AND          (             a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD')              OR              a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')             )         AND NOT COALESCE(a.CURVE_TYPE,'') IN ('CPCR_CLEAR', 'CPCRFUND', 'MS_SECCPM', 'CPCR_MPEFUND')         AND NOT a.PRODUCT_SUB_TYPE_CODE = 'CMBX'         AND NOT a.VERTICAL_SYSTEM LIKE 'SPG%'         AND a.USD_PV01SPRD IS NOT NULL         AND NOT a.CURVE_NAME = 'cpcrmne'         AND NOT a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX', 'CRDBSKT', 'CRDINDEX', 'LOANINDEX', 'MUNICDX')         AND NOT substr(a.CURVE_NAME,1,4) IN ('cpeu','cpjp','cpna','cpnj','cprw')         AND NOT substr(a.CURVE_NAME,1,3) IN ('ute')         AND NOT CURVE_NAME = 'comcr_wind'         AND NOT a.VERTICAL_SYSTEM LIKE 'C1%'          AND NOT a.VERTICAL_SYSTEM LIKE 'BANKLOANS%'         AND a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID IN (             SELECT                  a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID             FROM cdwuser.U_CR_MSR a             WHERE 1=1 and (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and                  (                     a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT','MS CVA MNE - FID', 'MS CVA MNE - COMMOD')                      OR                      a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')                     )                 AND NOT COALESCE(a.CURVE_TYPE,'') IN ('CPCR_CLEAR', 'CPCRFUND', 'MS_SECCPM', 'CPCR_MPEFUND')                 AND NOT a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX', 'CRDBSKT', 'CRDINDEX', 'LOANINDEX', 'MUNICDX')                 AND NOT a.PRODUCT_SUB_TYPE_CODE = 'CMBX'                 AND NOT a.VERTICAL_SYSTEM LIKE 'SPG%'                 AND a.USD_PV01SPRD IS NOT NULL                 AND NOT substr(a.CURVE_NAME,1,4) in ('cpeu','cpjp','cpna','cpnj','cprw')                 AND NOT substr(a.CURVE_NAME,1,3) in ('ute')                 AND NOT a.CURVE_NAME = 'comcr_wind'                 AND a.VERTICAL_SYSTEM like 'C1%'                 Group by                    a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         )     GROUP BY         a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE         , GENERIC_Flag UNION ALL     SELECT          a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE     , 'Net' AS CVA_Type_Flag     , 'N/A' AS GENERIC_Flag     , SUM(a.USD_PV01SPRD)/1000 AS PV1SPRD     FROM          cdwuser.U_CR_MSR a     WHERE 1=1 and (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and CCC_PL_REPORTING_REGION in ('EMEA') AND          (             a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD')              OR              a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')             )         AND NOT COALESCE(a.CURVE_TYPE,'') IN ('CPCR_CLEAR', 'CPCRFUND', 'MS_SECCPM', 'CPCR_MPEFUND')         AND NOT a.PRODUCT_SUB_TYPE_CODE = 'CMBX'         AND NOT a.VERTICAL_SYSTEM LIKE 'SPG%'         AND a.USD_PV01SPRD IS NOT NULL     GROUP BY         a.COB_DATE         , a.CURVE_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME         , a.POSITION_ULT_ISSUER_PARTY_DARWIN_ID         , a.VERTICAL_SYSTEM         , a.CURVE_TYPE         , a.PRODUCT_SUB_TYPE_CODE         , a.PRODUCT_TYPE_CODE