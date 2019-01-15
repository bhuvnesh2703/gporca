SELECT a.COB_DATE, a.CCC_BUSINESS_AREA, a.MRD_RATING,CCC_PL_REPORTING_REGION, CASE WHEN COALESCE(a.CURVE_TYPE,'') = 'CPCRMNE' THEN 'MS CDS' WHEN COALESCE(a.CURVE_TYPE,'') = 'MS_SECCPM' THEN 'MS Bond' WHEN COALESCE(a.CURVE_TYPE,'') IN ('CPCRFUND', 'CPCR_MPEFUND') THEN 'Dealer Bond' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('MPE', 'MPE_CVA', 'MNE', 'MNE_CVA', 'MNE_CP', 'MPE_PROXY', 'MPE_FVA', 'MPE_FVA_RAW', 'MNE_FVA_NET', 'MNE_FVA') THEN 'MPE CVA' WHEN a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX', 'CRDBSKT', 'CRDINDEX', 'LOANINDEX', 'MUNICDX') THEN 'INDEX' ELSE 'SN' END AS CVA_Type_Flag, CASE WHEN a.TERM_BUCKET in ('0.5-1Y','0-0.25Y','0.25-0.5Y','0-0.083Y','0.083-0.25Y', '0.5-0.75Y', '0.75-1Y') THEN '0-1Yr' WHEN a.TERM_BUCKET in ('1-2Y','2-5Y', '2-3Y','3-5Y') THEN '1-5Yr' WHEN a.TERM_BUCKET in ('5-7Y','7-8Y','8-10Y') THEN '5-10Yr' WHEN a.TERM_BUCKET in ('10-12Y', '12-15Y') THEN '10-15Yr' WHEN a.TERM_BUCKET in ('15-20Y','20-25Y','25-30Y') THEN '15-30Yr' WHEN a.TERM_BUCKET in ('30-40Y','40-50Y','50-60Y','60-75Y','75+Y') THEN '30Yr+' ELSE 'NULL' END AS term_new_group, Sum(usd_pv01sprd) AS USD_PV01SPRD FROM cdwuser.U_CR_MSR a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and LE_GROUP = ('UK') AND  (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) AND COALESCE(a.CURVE_TYPE,'') <> 'CPCR_CLEAR'  AND a.usd_pv01sprd IS NOT NULL GROUP BY a.COB_DATE, a.CCC_BUSINESS_AREA,CVA_Type_Flag, a.TERM_BUCKET, a.MRD_RATING, CCC_PL_REPORTING_REGION