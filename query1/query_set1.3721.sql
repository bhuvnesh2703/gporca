SELECT
    cob_date,
    ccc_division,
    ccc_pl_reporting_region,
    ccc_business_area,
    ccc_banking_trading,
LE_GROUP, VERTICAL_SYSTEM,
sum(USD_FX) as FX_DELTA,

case when (tr.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD','COMMODS FINANCING') 
OR tr.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES')) AND tr.CCC_PRODUCT_LINE NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE') then 'CVA_DELTA'


when tr.CCC_BUSINESS_AREA = 'LENDING' then 'LENDING'
 else 'FX_DELTA' end as CVA_FL

FROM cdwuser.u_exp_msr tr

WHERE
 cob_date IN ('2018-02-28','2018-02-21')
    AND tr.currency_of_measure IN ('EUR') 
    AND tr.USD_FX <> 0      
AND CCC_DIVISION NOT IN ('FID DVA', 'FIC DVA')
AND CCC_STRATEGY NOT IN ('MS DVA STR NOTES IED')


GROUP BY
   ccc_banking_trading,
    ccc_business_area,
    cob_date,
    ccc_division,
    ccc_pl_reporting_region,
    VERTICAL_SYSTEM,
    LE_GROUP,
case when (tr.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD','COMMODS FINANCING') 
OR tr.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES')) AND tr.CCC_PRODUCT_LINE NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE') then 'CVA_DELTA'
when tr.CCC_BUSINESS_AREA = 'LENDING' then 'LENDING'
 else 'FX_DELTA' end