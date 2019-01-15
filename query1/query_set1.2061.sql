WITH POPULATION AS ( select 1 as order, ccc_pl_reporting_region, case when risk_manager_location in ('HONG KONG','HONG KONG','SEOUL','SINGAPORE','TOKYO','ZHUHAI','SYDNEY') then 'ASIA PACIFIC' when risk_manager_location in ('ISTANBUL','LONDON','MOSCOW','PARIS','ZURICH') then 'EMEA' when risk_manager_location in ('MEXICO CITY','NEW YORK','SAO PAULO') then 'AMERICAS' else 'UNMAPPED' end as REGION, ccc_risk_manager_login, ccc_risk_mgr_name, risk_manager_location, BASEL_III_GROUP_BTI, Case when( CCC_BUSINESS_AREA IN ('CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) then 'CVA' else CCC_DIVISION end as CCC_DIVISION, Case when CCC_PRODUCT_LINE in ('CREL BANK HFI','CRE LENDING SEC/HFS','WAREHOUSE') Then 'WAREHOUSE AND CRE LENDING' When FID1_SENIORITY IN ('AT1', 'SUBT1', 'SUBUT2') Then 'JUNIOR SUBORDINATE' when CCC_PRODUCT_LINE IN ('DISTRESSED TRADING') then 'DISTRESSED TRADING' else CCC_BUSINESS_AREA end as CCC_BUSINESS_AREA, ccc_product_line, product_type_code, sum(case when cob_date ='2018-02-28' then usd_ir_unified_pv01 else 0 end) as usd_pv01_cob, sum(case when cob_date ='2018-02-28' then usd_ir_unified_pv01 else -usd_ir_unified_pv01 end) as usd_pv01_change, sum(case when (a.cob_date ='2018-02-28' and a.vertical_system like'%EQUITY%') then A.USD_PV01SPRD/1000 when cob_date ='2018-02-28' then A.USD_PV01SPRD else 0 end) as usd_pv01sprd_cob, sum(Case when (a.cob_date = '2018-02-28' AND a.vertical_system like'%EQUITY%') then A.USD_PV01SPRD/1000 when (a.cob_date = '2018-02-28') then A.USD_PV01SPRD when (a.cob_date = '2018-01-31' AND a.vertical_system like'%EQUITY%') then -A.USD_PV01SPRD/1000 when (a.cob_date = '2018-01-31') then A.USD_PV01SPRD else 0 end ) as usd_pv01sprd_change, sum(case when cob_date ='2018-02-28' then coalesce(A.USD_PV10_BENCH,a.USD_CREDIT_PV10PCT) else 0 end) as usd_pv10_bench_cob, sum(case when cob_date ='2018-02-28' then coalesce(A.USD_PV10_BENCH,a.USD_CREDIT_PV10PCT) else -coalesce(A.USD_PV10_BENCH,a.USD_CREDIT_PV10PCT) end) as usd_pv10_bench_change, sum(case when (cob_date ='2018-02-28' and currency_of_measure not in ('USD', 'UBD')) then usd_fx else 0 end ) as fx_delta_cob, sum(case when (cob_date ='2018-02-28' and currency_of_measure not in ('USD', 'UBD')) then usd_fx when (cob_date <> '2018-02-28' and currency_of_measure not in ('USD', 'UBD')) then -usd_fx else 0 end) as fx_delta_change, sum(case when cob_date ='2018-02-28' then USD_FX_KAPPA else 0 end) as fx_kappa_cob, sum(case when cob_date ='2018-02-28' then USD_FX_KAPPA else -USD_FX_KAPPA end) as fx_kappa_change, sum(case when cob_date ='2018-02-28' then usd_ir_kappa/10 else 0 end) as ir_vega_cob, sum(case when cob_date ='2018-02-28' then usd_ir_kappa/10 else -usd_ir_kappa/10 end) as ir_vega_change, sum(case when a.cob_date = '2018-02-28' then a.USD_EXPOSURE else 0 end) as net_exposure_cob, sum(case when a.cob_date = '2018-02-28' then a.USD_EXPOSURE else -a.USD_EXPOSURE end) as net_exposure_change, sum(case when cob_date ='2018-02-28' then usd_irpv01sprd else 0 end) as basis_pv01_cob, sum(case when cob_date ='2018-02-28' then usd_irpv01sprd else -usd_irpv01sprd end) as basis_pv01_change, sum(case when cob_date ='2018-02-28' then a.USD_DELTA else 0 end) as nramv_cob, sum(case when cob_date ='2018-02-28' then a.USD_DELTA else -a.USD_DELTA end) as nramv_change, sum(case when cob_date ='2018-02-28' then a.USD_CM_DELTA else 0 end) as cm_delta_cob, sum(case when cob_date ='2018-02-28' then a.USD_CM_DELTA else -a.USD_CM_DELTA end) as cm_delta_change, sum(case when cob_date ='2018-02-28' then a.usd_notional else 0 end) as usd_notional_cob, sum(case when cob_date ='2018-02-28' then a.usd_notional else 0 end) as usd_notional_change, sum(case when cob_date ='2018-02-28' then a.USD_CM_KAPPA else 0 end) as cm_kappa_cob, sum(case when cob_date ='2018-02-28' then a.USD_CM_KAPPA else -a.USD_CM_KAPPA end) as cm_kappa_change, sum(case when cob_date ='2018-02-28' then a.USD_TOTAL_COMMIT else 0 end) as commitment_cob, sum(case when cob_date ='2018-02-28' then a.USD_TOTAL_COMMIT else -a.USD_TOTAL_COMMIT end) as commitment_change from cdwuser.u_exp_msr a where cob_date in ('2018-02-28', '2018-01-31') and parent_legal_entity = '0302(G)' and CCC_DIVISION in ('COMMODITIES','FIXED INCOME DIVISION','NON CORE') and CCC_BUSINESS_AREA not in ('CPM') Group by ccc_pl_reporting_region, case when risk_manager_location in ('HONG KONG','HONG KONG','SEOUL','SINGAPORE','TOKYO','ZHUHAI','SYDNEY') then 'ASIA PACIFIC' when risk_manager_location in ('ISTANBUL','LONDON','MOSCOW','PARIS','ZURICH') then 'EMEA' when risk_manager_location in ('MEXICO CITY','NEW YORK','SAO PAULO') then 'AMERICAS' else 'UNMAPPED' end , ccc_risk_manager_login, ccc_risk_mgr_name, risk_manager_location, BASEL_III_GROUP_BTI,Case when( CCC_BUSINESS_AREA IN ('CPM','CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) then 'CVA' else CCC_DIVISION end, Case when CCC_PRODUCT_LINE in ('CREL BANK HFI','CRE LENDING SEC/HFS','WAREHOUSE') Then 'WAREHOUSE AND CRE LENDING' When FID1_SENIORITY IN ('AT1', 'SUBT1', 'SUBUT2') Then 'JUNIOR SUBORDINATE' when CCC_PRODUCT_LINE IN ('DISTRESSED TRADING') then 'DISTRESSED TRADING' else CCC_BUSINESS_AREA end, ccc_product_line, product_type_code ) SELECT coalesce(risk_manager_location,'Non-London Locations') as risk_manager_location, 1 as order, ccc_pl_reporting_region, REGION, ccc_risk_manager_login, ccc_risk_mgr_name, BASEL_III_GROUP_BTI, CCC_DIVISION, CCC_BUSINESS_AREA, ccc_product_line, product_type_code, sum(usd_pv01_cob) as usd_pv01_cob, sum(usd_pv01_change) as usd_pv01_change, sum(usd_pv01sprd_cob) as usd_pv01sprd_cob, sum(usd_pv01sprd_change) as usd_pv01sprd_change, sum(usd_pv10_bench_cob) as usd_pv10_bench_cob, sum(usd_pv10_bench_change) as usd_pv10_bench_change, sum(fx_delta_cob) as fx_delta_cob, sum(fx_delta_change) as fx_delta_change, sum(fx_kappa_cob) as fx_kappa_cob, sum(fx_kappa_change) as fx_kappa_change, sum(ir_vega_cob) as ir_vega_cob, sum(ir_vega_change) as ir_vega_change, sum(net_exposure_cob) as net_exposure_cob, sum(net_exposure_change) as net_exposure_change, sum(basis_pv01_cob) as basis_pv01_cob, sum(basis_pv01_change) as basis_pv01_change, sum(nramv_cob) as nramv_cob, sum(nramv_change) as nramv_change, sum(cm_delta_cob) as cm_delta_cob, sum(cm_delta_change) as cm_delta_change, sum(usd_notional_cob) as usd_notional_cob, sum(usd_notional_change) as usd_notional_change, sum(cm_kappa_cob) as cm_kappa_cob, sum(cm_kappa_change) as cm_kappa_change, sum(commitment_cob) as commitment_cob, sum(commitment_change) as commitment_change FROM POPULATION WHERE risk_manager_location<>'LONDON' GROUP BY ROLLUP(risk_manager_location), 2,3,4,5,6,7,8,9,10,11 UNION ALL SELECT risk_manager_location, 20 as order, ccc_pl_reporting_region, REGION, ccc_risk_manager_login, ccc_risk_mgr_name, BASEL_III_GROUP_BTI, CCC_DIVISION, CCC_BUSINESS_AREA, ccc_product_line, product_type_code, sum(usd_pv01_cob) as usd_pv01_cob, sum(usd_pv01_change) as usd_pv01_change, sum(usd_pv01sprd_cob) as usd_pv01sprd_cob, sum(usd_pv01sprd_change) as usd_pv01sprd_change, sum(usd_pv10_bench_cob) as usd_pv10_bench_cob, sum(usd_pv10_bench_change) as usd_pv10_bench_change, sum(fx_delta_cob) as fx_delta_cob, sum(fx_delta_change) as fx_delta_change, sum(fx_kappa_cob) as fx_kappa_cob, sum(fx_kappa_change) as fx_kappa_change, sum(ir_vega_cob) as ir_vega_cob, sum(ir_vega_change) as ir_vega_change, sum(net_exposure_cob) as net_exposure_cob, sum(net_exposure_change) as net_exposure_change, sum(basis_pv01_cob) as basis_pv01_cob, sum(basis_pv01_change) as basis_pv01_change, sum(nramv_cob) as nramv_cob, sum(nramv_change) as nramv_change, sum(cm_delta_cob) as cm_delta_cob, sum(cm_delta_change) as cm_delta_change, sum(usd_notional_cob) as usd_notional_cob, sum(usd_notional_change) as usd_notional_change, sum(cm_kappa_cob) as cm_kappa_cob, sum(cm_kappa_change) as cm_kappa_change, sum(commitment_cob) as commitment_cob, sum(commitment_change) as commitment_change FROM POPULATION WHERE risk_manager_location='LONDON' GROUP BY 1,2,3,4,5,6,7,8,9,10,11 UNION ALL SELECT 'Grand Total' as risk_manager_location, 30 as order, ccc_pl_reporting_region, REGION, ccc_risk_manager_login, ccc_risk_mgr_name, BASEL_III_GROUP_BTI, CCC_DIVISION, CCC_BUSINESS_AREA, ccc_product_line, product_type_code, sum(usd_pv01_cob) as usd_pv01_cob, sum(usd_pv01_change) as usd_pv01_change, sum(usd_pv01sprd_cob) as usd_pv01sprd_cob, sum(usd_pv01sprd_change) as usd_pv01sprd_change, sum(usd_pv10_bench_cob) as usd_pv10_bench_cob, sum(usd_pv10_bench_change) as usd_pv10_bench_change, sum(fx_delta_cob) as fx_delta_cob, sum(fx_delta_change) as fx_delta_change, sum(fx_kappa_cob) as fx_kappa_cob, sum(fx_kappa_change) as fx_kappa_change, sum(ir_vega_cob) as ir_vega_cob, sum(ir_vega_change) as ir_vega_change, sum(net_exposure_cob) as net_exposure_cob, sum(net_exposure_change) as net_exposure_change, sum(basis_pv01_cob) as basis_pv01_cob, sum(basis_pv01_change) as basis_pv01_change, sum(nramv_cob) as nramv_cob, sum(nramv_change) as nramv_change, sum(cm_delta_cob) as cm_delta_cob, sum(cm_delta_change) as cm_delta_change, sum(usd_notional_cob) as usd_notional_cob, sum(usd_notional_change) as usd_notional_change, sum(cm_kappa_cob) as cm_kappa_cob, sum(cm_kappa_change) as cm_kappa_change, sum(commitment_cob) as commitment_cob, sum(commitment_change) as commitment_change FROM POPULATION GROUP BY 1,2,3,4,5,6,7,8,9,10,11