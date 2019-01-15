Select COB_DATE, a.CCC_GL_COMPANY_CODE, PRODUCT_TYPE_CODE, a.book, Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable' when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG')) Then 'HFS Hedgeable' when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging' when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS' when BOOK like '%ELTH FVO%' Then 'EventRel FVO' when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable' when (BOOK like 'REL%' or BOOK = 'PMGPB') Then 'Relationship Legacy' when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - NIG WORKOUT HFI MSBNA-LIWMB'))Then 'EventRel HFI' when BOOK like 'HFI%' Then 'HFI' Else 'Other' END as Level6 , Case when PRODUCT_TYPE_CODE = 'BANKDEBT' Then 'Loan' Else PRODUCT_TYPE_CODE END as type , Case when MRD_RATING in ('AAA', 'AA', 'A', 'BBB') Then 'IG' Else 'HY' END as rating , Case when PRODUCT_TYPE_CODE in ('CDSOPTIDX', 'CRDINDEX') Then sum(coalesce((SLIDE_PVSPRCOMP_PLS_50PCT_USD) :: numeric(15,5),0)) ELSE sum(coalesce((a.SLIDE_PV_PLS_50PCT_USD) :: numeric(15,5),0)) END as PV50composite , sum(coalesce((a.USD_PV01sprd) :: numeric(15,5),0)) as USD_PV01SPRD , sum(coalesce((USD_PV10_BENCH_COMP) :: numeric(15,5),0)) as PV10 , sum(coalesce((SLIDE_PVSPRCOMP_PLS_50PCT_USD) :: numeric(15,5),0)) as pv50comp , sum(coalesce((a.SLIDE_PV_PLS_50PCT_USD) :: numeric(15,5),0)) as pv50 , sum(coalesce((a.USD_NOTIONAL) :: numeric(15,5),0)) as USD_Notional , sum(coalesce((a.USD_IR_UNIFIED_PV01) :: numeric(15,5),0)) USD_PV01 from cdwuser.U_EXP_MSR a where a.COB_DATE in ( '2018-02-28', '2018-01-31', '2017-12-29', '2017-11-30', '2018-01-30', '2017-10-31', '2017-09-29' ) and a.CCC_GL_COMPANY_CODE = '1633' and (CCC_BUSINESS_AREA = 'LENDING' or (CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' and CCC_PRODUCT_LINE = 'PRIMARY - LOANS')) and CCC_STRATEGY not in ('CORPORATE LOAN STRATEGY','PROJECT FINANCE','EVENT - INV GRADE','EVENT - NON INV GRADE') and a.VERTICAL_SYSTEM not like 'PIPELINE%' Group by COB_DATE, a.CCC_GL_COMPANY_CODE, PRODUCT_TYPE_CODE, a.book, Case when ((BOOK like 'HFS%' and BOOK like '%UNHEDG%') or BOOK in ('BIXLU', 'BIXNU', 'BNYUU'))Then 'HFS Unhedgeable' when ((BOOK like 'HFS%' and BOOK like '%HEDG%' and BOOK not like '%UNHEDG%') or BOOK in ('ALOAN', 'APLJV', 'ASCLV', 'BALON', 'BHYLG', 'BLNUT', 'BNYUT', 'MSBIP', 'HYLLG')) Then 'HFS Hedgeable' when BOOK in ('HFIEA','HFIET','HFIEU','HFINA','HFINT','HFIEB', 'HFINB') Then 'HFI Hedging' when ((BOOK like '%LAF%' and BOOK like '%ELTH HFS%') or BOOK = 'LAF - NA - NIG WORKOUT HFS MSSFI-LNWMF') Then 'EventRel HFS' when BOOK like '%ELTH FVO%' Then 'EventRel FVO' when (BOOK like 'REL%' and BOOK like '%UNHEDG%') Then 'HFS Unhedgeable' when (BOOK like 'REL%' or BOOK = 'PMGPB') Then 'Relationship Legacy' when (BOOK like '%ELTH HFI%' or BOOK in ('LAF - NA - NIG ELTH WORKOUT HFI MSBNA-LIWMB', 'LAF - NA - PCIF ELTH WORKOUT HFI MSBNA-LPFWB'))Then 'EventRel HFI' when BOOK like 'HFI%' Then 'HFI' Else 'Other' END , Case when PRODUCT_TYPE_CODE = 'BANKDEBT' Then 'Loan' Else PRODUCT_TYPE_CODE END , Case when MRD_RATING in ('AAA', 'AA', 'A', 'BBB') Then 'IG' Else 'HY' END