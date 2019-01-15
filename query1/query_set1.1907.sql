SELECT 'BAU' AS scenario_group ,cob_date ,SUM(scenario_pnl) AS pnl FROM dwuser.u_modular_scenarios_ccar where COB_DATE = '2018-02-28' AND (condition_name LIKE '%DIRECTIONAL_GOVT_II' OR condition_name LIKE '%DIR_GOVT_II') AND stress_scenario IN ('BAU2017_S1') AND risk_system NOT LIKE '%SILO' AND ( CCAR_BUSINESS_CATEGORY NOT LIKE 'NOT%' OR CCC_BUSINESS_AREA = 'GLOBAL EQUITY ADMIN & DEV' ) AND scenario_type = 'GREEK' AND attribution NOT LIKE '%GAMMA%' GROUP BY cob_date UNION ALL SELECT 'BAU' AS scenario_group ,cob_date ,SUM(scenario_pnl) AS pnl FROM dwuser.u_modular_scenarios_ccar where COB_DATE = '2018-01-31' AND (condition_name LIKE '%DIRECTIONAL_GOVT_II' OR condition_name LIKE '%DIR_GOVT_II') AND stress_scenario IN ('BAU2017_S1') AND risk_system NOT LIKE '%SILO' AND ( CCAR_BUSINESS_CATEGORY NOT LIKE 'NOT%' OR CCC_BUSINESS_AREA = 'GLOBAL EQUITY ADMIN & DEV' ) AND scenario_type = 'GREEK' AND attribution NOT LIKE '%GAMMA%' GROUP BY cob_date