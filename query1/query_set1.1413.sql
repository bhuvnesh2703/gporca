select cut, COB_DATE, CCC_PL_REPORTING_REGION, SUM(D30) AS D30, SUM(D20) AS D20, SUM(D10) AS D10, SUM(D8) AS D8, SUM(D5) AS D5, SUM(D3) AS D3, SUM(D2) AS D2, SUM(D1) AS D1, SUM(P1) AS P1, SUM(P2) AS P2, SUM(P3) AS P3, SUM(P5) AS P5, SUM(P8) AS P8, SUM(P10) AS P10, SUM(P20) AS P20 FROM ( select unnest(array[cut1,cut2,cut3])cut, COB_DATE, CCC_PL_REPORTING_REGION, D30, D20, D10, D8, D5, D3, D2, D1, P1, P2, P3, P5, P8, P10, P20 FROM ( SELECT CASE WHEN a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' END as Cut1, CASE WHEN a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND LE_GROUP = 'UK' THEN 'UK GROUP' END as Cut2, CASE WHEN a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' THEN 'GLOBAL' END as Cut3, a.COB_DATE, CASE WHEN a.CCC_PL_REPORTING_REGION = 'EMEA' THEN 'EMEA' WHEN a.CCC_PL_REPORTING_REGION = 'AMERICAS' THEN 'AMERICAS' ELSE 'ASIA Incl. Japan' END as CCC_PL_REPORTING_REGION, SUM(CASE WHEN a.SCENARIO_NAME = 'S-30%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS D30, SUM(CASE WHEN a.SCENARIO_NAME = 'S-20%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS D20, SUM(CASE WHEN a.SCENARIO_NAME = 'S-10%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS D10, SUM(CASE WHEN a.SCENARIO_NAME = 'S-8%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS D8, SUM(CASE WHEN a.SCENARIO_NAME = 'S-5%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS D5, SUM(CASE WHEN a.SCENARIO_NAME = 'S-3%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS D3, SUM(CASE WHEN a.SCENARIO_NAME = 'S-2%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS D2, SUM(CASE WHEN a.SCENARIO_NAME = 'S-1%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS D1, SUM(CASE WHEN a.SCENARIO_NAME = 'S+1%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS P1, SUM(CASE WHEN a.SCENARIO_NAME = 'S+2%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS P2, SUM(CASE WHEN a.SCENARIO_NAME = 'S+3%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS P3, SUM(CASE WHEN a.SCENARIO_NAME = 'S+5%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS P5, SUM(CASE WHEN a.SCENARIO_NAME = 'S+8%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS P8, SUM(CASE WHEN a.SCENARIO_NAME = 'S+10%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS P10, SUM(CASE WHEN a.SCENARIO_NAME = 'S+20%' THEN coalesce(a.POS_TV_MINUS_BASE,0)END )/1000 AS P20 FROM CDWUSER.U_SURFACE_MSR a WHERE a.CCC_BANKING_TRADING = 'TRADING' AND a.COB_DATE IN ('2018-02-28','2018-01-31') AND a.SCENARIO_NAME in ('S-30%','S-20%','S-10%','S-8%','S-5%','S-3%','S-2%','S-1%','S+1%','S+2%','S+3%','S+5%','S+8%','S+10%','S+20%') GROUP BY a.COB_DATE,cut1,cut2,cut3,a.CCC_PL_REPORTING_REGION )a )b where cut is not null group by cut,cob_date,CCC_PL_REPORTING_REGION order by 1,2