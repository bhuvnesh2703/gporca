SELECT COB_DATE, sum(PV01xSPD)/sum(PV01) as WA_CPSpread  from  (SELECT a.COB_DATE,  POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ,POSITION_ULT_ISSUER_PARTY_DARWIN_ID,  CREDIT_SPREAD, SUM(a.USD_PV01SPRD) AS PV01, SUM(a.USD_PV01SPRD) *CREDIT_SPREAD AS PV01xSPD  FROM cDWUSER.U_CR_MSR a  WHERE (a.COB_DATE in ('2/28/2018', '2/27/2018', '1/31/2018', '12/29/2017', '12/29/2017', '9/29/2017', '6/30/2017', '3/31/2017', '12/30/2016', '9/30/2016')) AND(a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT','COMMODS FINANCING', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD')   OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING'))  AND NOT COALESCE(a.CURVE_TYPE,'') IN ('CPCRMNE',  'MS_SECCPM', 'CPCRFUND', 'CPCR_MPEFUND','CPCR_CLEAR')  AND a.PRODUCT_SUB_TYPE_CODE IN ('MPE', 'MPE_CVA', 'MNE', 'MNE_CVA', 'MNE_CP', 'MPE_PROXY', 'MPE_FVA', 'MPE_FVA_RAW', 'MNE_FVA_NET', 'MNE_FVA')   AND (a.USD_PV01SPRD IS NOT NULL AND CREDIT_SPREAD <>0)  GROUP BY  a.COB_DATE,  POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ,POSITION_ULT_ISSUER_PARTY_DARWIN_ID ,CREDIT_SPREAD) as B group by COB_DATE