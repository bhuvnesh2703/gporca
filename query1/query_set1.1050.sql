SELECT     b.COB_DATE,     b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,     b.PRODUCT_TYPE_CODE,     SUM (B.BPV10) AS BPV10 FROM     (         SELECT             A.COB_DATE,             A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,             a.PRODUCT_TYPE_CODE,             CASE WHEN COALESCE (a.CURVE_TYPE,                                 '') = 'CPCRMNE' THEN 'MS CDS' WHEN COALESCE (a.CURVE_TYPE,                                                                              '') = 'MS_SECCPM' THEN 'MS Bond' WHEN COALESCE (a.                                                                                                                                  CURVE_TYPE                                                                                                                                  ,                                                                                                                              '') IN (                                                                                                                                          'CPCRFUND'                 , 'CPCR_MPEFUND') THEN 'Dealer Bond' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('MPE', 'MPE_CVA', 'MNE', 'MNE_CVA', 'MNE_CP',                                                                                            'MPE_PROXY', 'MPE_FVA_PROXY', 'MPE_FVA',                                                                                            'MPE_FVA_RAW', 'MNE_FVA_NET', 'MNE_FVA') THEN                  'MPE CVA' WHEN a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX', 'CRDBSKT', 'CRDINDEX', 'LOANINDEX', 'MUNICDX') THEN 'INDEX'             ELSE 'SN' END AS CVA_Type_Flag,             SUM (A.USD_PV10_BENCH) AS BPV10         FROM cdwuser.U_DM_CVA A         WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-27') and IS_UK_GROUP = 'Y' AND              A.CCC_STRATEGY_DETAILS = 'INSURANCE PRODUCTS' AND             A.USD_PV10_BENCH IS NOT NULL         GROUP BY             a.COB_DATE,             A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,             a.PRODUCT_TYPE_CODE,             A.MPE_FLAG,             A.CURVE_TYPE,             CASE WHEN COALESCE (a.CURVE_TYPE,                                 '') = 'CPCRMNE' THEN 'MS CDS' WHEN COALESCE (a.CURVE_TYPE,                                                                              '') = 'MS_SECCPM' THEN 'MS Bond' WHEN COALESCE (a.                                                                                                                                  CURVE_TYPE                                                                                                                                  ,                                                                                                                              '') IN (                                                                                                                                          'CPCRFUND'                 , 'CPCR_MPEFUND') THEN 'Dealer Bond' WHEN a.PRODUCT_SUB_TYPE_CODE IN ('MPE', 'MPE_CVA', 'MNE', 'MNE_CVA', 'MNE_CP',                                                                                            'MPE_PROXY', 'MPE_FVA_PROXY', 'MPE_FVA',                                                                                            'MPE_FVA_RAW', 'MNE_FVA_NET', 'MNE_FVA') THEN                  'MPE CVA' WHEN a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX', 'CRDBSKT', 'CRDINDEX', 'LOANINDEX', 'MUNICDX') THEN 'INDEX'             ELSE 'SN' END     )     AS B GROUP BY     b.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,     b.PRODUCT_TYPE_CODE,     b.COB_DATE