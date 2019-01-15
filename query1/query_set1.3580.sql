SELECT SUM(V_M050) AS MINUS50PCT, SUM((COALESCE(V_M050,0)) + (-0.25+0.5)*(COALESCE(V_M019,0)-COALESCE(V_M050,0))/(-0.19+0.5)) AS MINUS25PCT, SUM(V_M010) AS MINUS10PCT, SUM(V_P010) AS PLUS10PCT, SUM((COALESCE(V_P021,0)) + (0.25-0.21)*(COALESCE(V_P050,0)-COALESCE(V_P021,0))/(0.5-0.21)) AS PLUS25PCT, SUM(V_P050) AS PLUS50PCT, SUM(V_P100) AS PLUS100PCT, SUM(V_P200) AS PLUS200PCT, SUM(V_P300) AS PLUS300PCT FROM ( SELECT CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, CCC_STRATEGY, SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_MIN_50PCT_USD,0) ELSE COALESCE(SLIDE_PV_MIN_50PCT_USD,0) END) AS V_M050, SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_MIN_19PCT_USD,0) ELSE COALESCE(SLIDE_PV_MIN_19PCT_USD,0) END) AS V_M019 , SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_MIN_10PCT_USD,0) ELSE COALESCE(SLIDE_PV_MIN_10PCT_USD,0) END) AS V_M010, SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_PLS_10PCT_USD,0) ELSE COALESCE(SLIDE_PV_PLS_10PCT_USD,0) END) AS V_P010, SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_PLS_21PCT_USD,0) ELSE COALESCE(SLIDE_PV_PLS_21PCT_USD,0) END) AS V_P021, SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_PLS_50PCT_USD,0) ELSE COALESCE(SLIDE_PV_PLS_50PCT_USD,0) END) AS V_P050, SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_PLS_100PCT_USD,0) ELSE COALESCE(SLIDE_PV_PLS_100PCT_USD,0) END) AS V_P100, SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_PLS_200PCT_USD,0) ELSE COALESCE(SLIDE_PV_PLS_200PCT_USD,0) END) AS V_P200, SUM(CASE WHEN b.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX','MUNICDX','CRDBSKT') THEN COALESCE(b.SLIDE_PVSPRCOMP_PLS_300PCT_USD,0) ELSE COALESCE(SLIDE_PV_PLS_300PCT_USD,0) END) AS V_P300 FROM cdwuser.U_CR_MSR b WHERE cob_date IN ('2018-02-28') AND CCC_BUSINESS_AREA IN ('CREDIT-CORPORATES','MUNICIPAL SECURITIES', 'EM CREDIT TRADING') AND CCC_STRATEGY NOT IN ('HELD FOR INVESTMENT','PROJECT FINANCE', 'CORPORATE LOAN STRATEGY') AND BOOK NOT IN ('PMGPB') AND UPPER(book) not like '%WORKOUT%' AND LE_GROUP = 'UK' AND CCC_DIVISION NOT IN ('FID DVA','FIC DVA') AND CCC_STRATEGY NOT IN ('MS DVA STR NOTES IED') GROUP BY CCC_BUSINESS_AREA, CCC_PRODUCT_LINE, CCC_STRATEGY ) abc