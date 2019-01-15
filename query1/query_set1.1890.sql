SELECT     cob_date,     CASE WHEN ccc_business_area = 'LIQUID FLOW RATES' THEN ccc_product_line     ELSE ccc_strategy END AS reporting_line,     STRESS_SCENARIO,     SUM (scenario_pnl) AS pnl FROM dwuser.u_modular_scenarios_ccar  where COB_DATE = '2018-02-28' and risk_system NOT LIKE '%SILO' AND     (CCAR_BUSINESS_CATEGORY NOT LIKE 'NOT%' OR      CCC_BUSINESS_AREA = 'GLOBAL EQUITY ADMIN & DEV') AND     stress_scenario IN ('BHC2017_S1_IR_TENOR', 'FRB2017_SA_IR_TENOR', 'BAU2017_S1_IR_TENOR') AND     UPPER (ccc_business_area) IN ('LIQUID FLOW RATES', 'STRUCTURED RATES') GROUP BY     cob_date,     stress_scenario,     ccc_business_area,     ccc_product_line,     ccc_strategy  union all  SELECT     cob_date,     CASE WHEN ccc_business_area = 'LIQUID FLOW RATES' THEN ccc_product_line     ELSE ccc_strategy END AS reporting_line,     STRESS_SCENARIO,     SUM (scenario_pnl) AS pnl FROM dwuser.u_modular_scenarios_ccar  where COB_DATE = '2018-01-31' and risk_system NOT LIKE '%SILO' AND     (CCAR_BUSINESS_CATEGORY NOT LIKE 'NOT%' OR      CCC_BUSINESS_AREA = 'GLOBAL EQUITY ADMIN & DEV') AND     stress_scenario IN ('BHC2017_S1_IR_TENOR', 'FRB2017_SA_IR_TENOR', 'BAU2017_S1_IR_TENOR') AND     UPPER (ccc_business_area) IN ('LIQUID FLOW RATES', 'STRUCTURED RATES') GROUP BY     cob_date,     stress_scenario,     ccc_business_area,     ccc_product_line,     ccc_strategy