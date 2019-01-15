select a.cob_date, a.book, a.product_type_code, CASE WHEN a.book in ('AFSC1') and a.PRODUCT_TYPE_CODE in ('ABS', 'AGNCMO', 'BOND', 'CLO', 'CMBS', 'GVTBOND', 'REPO', 'RMBS') THEN 'CREDIT CORP' WHEN A.BOOK IN ('BKCAR') AND A.PRODUCT_TYPE_CODE IN ('ABS', 'AGNCMO', 'BOND', 'CLO', 'GVTBOND', 'REPO', 'RMBS') THEN 'CREDIT AUTO' WHEN A.BOOK IN ('BKCLO') AND A.PRODUCT_TYPE_CODE IN ('AGNCMO', 'BOND', 'CLO', 'CMBS', 'GVTBOND', 'REPO', 'RMBS') THEN 'CREDIT CLO' WHEN A.BOOK IN ('BKCMB') AND A.PRODUCT_TYPE_CODE IN ('ABS', 'AGNCMO', 'BOND', 'CLO', 'CMBS', 'GVTBOND', 'REPO', 'RMBS') THEN 'CREDIT CMBS' ELSE 'OTHER' END AS AFS_TYPE, COALESCE((USD_NOTIONAL) :: numeric(15,5), 0)/1000 as USD_NOTIONAL, COALESCE((USD_EXPOSURE) :: numeric(15,5), 0)/1000 as USD_EXPOSURE, COALESCE((USD_IR_UNIFIED_PV01) :: numeric(15,5),0) as USD_PV01, COALESCE((USD_PV01SPRD) :: numeric(15,5), 0) as USD_PV01SPRD, COALESCE((USD_PV10_BENCH) :: numeric(15,5), 0) as USD_PV10_BENCH from CDWUSER.U_DM_WM A where a.cob_date in ( '2018-02-28', '2018-02-27', '2018-01-31', '2017-12-29', '2017-11-30', '2017-10-31' ) and a.book in ('BKCAR', 'BKCLO', 'BKCMB', 'AFSC1') and a.ccc_taps_company in ('1633')