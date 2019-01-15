Select a.cob_date, a.GRID_MEASURE_NAME, case 
when (GRID_MEASURE_NAME in ('USD_GAMMA') and val_in <= 0.25) then 0.25
when (GRID_MEASURE_NAME in ('USD_GAMMA') and val_in <= 0.5) then 0.5
when (GRID_MEASURE_NAME in ('USD_GAMMA') and val_in <= 1) then 1
when (GRID_MEASURE_NAME in ('USD_GAMMA') and val_in <= 2) then 2
when (GRID_MEASURE_NAME in ('USD_GAMMA') and val_in <= 5) then 5
when (GRID_MEASURE_NAME in ('USD_GAMMA') and val_in <= 10) then 10
when (GRID_MEASURE_NAME in ('USD_GAMMA') and val_in > 10) then 20
else a.val_in end as val_in, a.val_for, Case when UPPER(CURVE_NAME) like ('EUR%') then 'EUR' when UPPER(CURVE_NAME) like ('GBP%') then 'GBP' when UPPER(CURVE_NAME) like ('USD%') then 'USD' when UPPER(CURVE_NAME) like ('JPY%') then 'JPY' when UPPER(CURVE_NAME) like ('CH%') then 'CHF' else 'Others' end as CURRENCY, sum(case when GRID_MEASURE_NAME in ('USD_GAMMA','USD_BREAKEVEN_SKEW','USD_BREAKEVEN_VOLOFVOL') then GRID_MEASURE_VALUE else GRID_MEASURE_VALUE/10 end) as GRID_MEASURE_VALUE FROM cdwuser.U_GRID_MSR A WHERE cob_date in ('2018-02-28','2018-02-27') AND ccc_product_line IN ('STRUCTURED RATES - EU') and a.GRID_MEASURE_NAME in ('USD_KAPPA','USD_GAMMA','USD_BREAKEVEN_KAPPA','USD_BREAKEVEN_SKEW','USD_BREAKEVEN_VOLOFVOL') group by a.cob_date, a.GRID_MEASURE_NAME, a.val_in, a.val_for, a.CURVE_NAME