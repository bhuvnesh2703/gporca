SELECT
    currency_of_measure,
    cob_date,
    ccc_division,
    ccc_pl_reporting_region,
    ccc_business_area,
    LE_GROUP,
    case when (tr.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD','COMMODS FINANCING') 
OR tr.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES')) AND tr.CCC_PRODUCT_LINE NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE') then 'CVA_FX_VEGA'
when tr.CCC_BUSINESS_AREA = 'LENDING' then 'LENDING'
 else 'FX_VEGA' end as CVA_FL,
    SUM(CASE WHEN tr.VERTICAL_SYSTEM like '%STS%' then coalesce(tr.USD_FX_PARTIAL_KAPPA,0) else coalesce(tr.USD_FX_KAPPA,0) end) as FX_KAPPA,
    SUM(CASE WHEN tr.VERTICAL_SYSTEM like '%STS%' then coalesce(tr.USD_FX_PARTIAL_KAPPA*measure_mark,0) else coalesce(tr.USD_FX_KAPPA*measure_mark,0) end) as FX_KAPPA_MARK

FROM cdwuser.u_exp_msr tr
WHERE
 cob_date IN ('2018-02-28','2018-02-21')
    AND tr.CURRENCY_OF_MEASURE like ('%EUR%')     

AND CCC_DIVISION NOT IN ('FID DVA', 'FIC DVA')
AND CCC_STRATEGY NOT IN ('MS DVA STR NOTES IED')

GROUP BY
case when (tr.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD','COMMODS FINANCING') 
OR tr.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES')) AND tr.CCC_PRODUCT_LINE NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE') then 'CVA_FX_VEGA'
when tr.CCC_BUSINESS_AREA = 'LENDING' then 'LENDING'
 else 'FX_VEGA' end,
    currency_of_measure,
    cob_date,
    ccc_division,
    ccc_pl_reporting_region,
    ccc_business_area,
    LE_GROUP