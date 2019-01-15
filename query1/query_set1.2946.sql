with scen as(     select          '2018-02-21' as cob_date,         SCENARIO_NAME,         ccc_business_area,         ccc_product_line,         sum(POS_TV_MINUS_BASE) as scenario_pnl     from         CDWUSER.u_ied_surface s     WHERE         cob_date in ('2018-02-21') and         s.CCC_DIVISION IN ('INSTITUTIONAL EQUITY DIVISION') AND         s.CCC_BANKING_TRADING IN ('TRADING') AND         s.SILO_SRC = 'IED' AND         s.ccc_business_area IN ('DERIVATIVES') AND         s.ccc_product_line IN ('EXOTIC PRODUCTS','CORPORATE EQUITY PRODUCTS') AND         s.SCENARIO_NAME IN          ('BASE', 'V-.05', 'V+.05', 'V+.1', 'V+.2', 'V+.3',         'S+5%', 'S+5%/V-.05', 'S+5%/V+.05', 'S+5%/V+.1',         'S+10%', 'S+10%/V-.05', 'S+10%/V+.05', 'S+10%/V+.1',          'S+20%', 'S+20%/V-.05',         'S+30%', 'S+30%/V-.05',          'S-5%', 'S-5%/V-.05', 'S-5%/V+.05', 'S-5%/V+.1', 'S-5%/V+.2','S-5%/V+.3',         'S-10%', 'S-10%/V-.05', 'S-10%/V+.05', 'S-10%/V+.1', 'S-10%/V+.2','S-10%/V+.3',         'S-20%', 'S-20%/V+.05', 'S-20%/V+.1', 'S-20%/V+.2', 'S-20%/V+.3',         'S-30%', 'S-30%/V+.05', 'S-30%/V+.1', 'S-30%/V+.2','S-30%/V+.3')     group by          cob_date,         SCENARIO_NAME,         ccc_business_area,         ccc_product_line ), delta as(     select         cob_date,         ccc_business_area,         ccc_product_line,         sum(coalesce(a.USD_CM_DELTA,0)+coalesce(a.USD_DELTA,0))  as delta     From         CDWUSER.U_DM_EQ a      where         cob_date in ('2018-02-21') and         a.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION'  AND          a.CCC_BANKING_TRADING<>'BANKING' AND         a.SILO_SRC = 'IED' AND         a.ccc_business_area IN ('DERIVATIVES') AND         a.ccc_product_line IN ('EXOTIC PRODUCTS','CORPORATE EQUITY PRODUCTS')     group by          ccc_business_area ,          ccc_product_line,         cob_date ) select     s.*,     d.delta,     s.scenario_pnl-     (delta*         case             when SUBSTRING(scenario_name,1,4)='S+5%' then 0.05             when SUBSTRING(scenario_name,1,4)='S-5%' then -0.05             when SUBSTRING(scenario_name,1,5)='S+10%' then 0.1             when SUBSTRING(scenario_name,1,5)='S-10%' then -0.1             when SUBSTRING(scenario_name,1,5)='S+20%' then 0.2             when SUBSTRING(scenario_name,1,5)='S-20%' then -0.2             when SUBSTRING(scenario_name,1,5)='S+30%' then 0.3             when SUBSTRING(scenario_name,1,5)='S-30%' then -0.3         else 0 end     ) as delta_pnl from     scen s left join     delta d on      s.COB_DATE=d.COB_DATE and     s.CCC_BUSINESS_AREA=d.CCC_BUSINESS_AREA and     s.CCC_PRODUCT_LINE=d.CCC_PRODUCT_LINE