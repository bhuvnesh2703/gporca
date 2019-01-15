SELECT cob_date, TERM_BUCKET_AGGR AS TERM_BUCKET, mrd_rating, SUM (COALESCE (((usd_ir_unified_pv01) :: numeric(15,5)), 0)) AS usd_ir_unified_pv01, SUM (COALESCE (((usd_pv01sprd) :: numeric(15,5)), 0)) AS usd_pv01sprd FROM CDWUSER.U_DM_WM WHERE cob_date IN ( '2018-02-28', '2018-02-27', '2018-01-31', '2017-12-29', '2017-11-30', '2017-10-31', '2017-09-29', '2017-08-31' ) AND ccc_business_area = 'NON CORE MARKETS' GROUP BY cob_date, mrd_rating, TERM_BUCKET_AGGR