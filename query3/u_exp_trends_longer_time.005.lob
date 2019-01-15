SELECT
    COB_DATE,
    PRODUCT_TYPE_CODE,
    PRODUCT_SUB_TYPE_CODE,
    CCC_PL_REPORTING_REGION,
    CCC_TAPS_COMPANY,
    COUNTRY_CD_OF_RISK,
    COUNTRY_OF_RISK_REGION,
    CCC_DIVISION,
    CCC_BUSINESS_AREA,
    CCC_PRODUCT_LINE,
    CVA_FL,
    HEDGE_STRATEGY,
    CURRENCY_CODE,
    CASE WHEN ccc_taps_company IN ('7800', '4068', '8962', '8961', '8959', '8941', '8790', '8789', '8772', '8757', '8627', '8564', 
                                       '8537', '8524', '8441', '8292', '8290', '8284', '8275', '8253', '8237', '8179', '8174', '7716', 
                                       '7705', '7704', '7458', '7435', '7416', '7281', '7280', '7043', '7016', '6899', '6893', '6838', 
                                       '6837', '6590', '6589', '6384', '6383', '6376', '6374', '6367', '6325', '6316', '6262', '6158', 
                                       '6157', '6120', '6114', '6036', '5869', '5856', '5656', '5614', '5357', '5310', '5274', '5254', 
                                       '5181', '5180', '5148', '5121', '5104', '5103', '4884', '4880', '4876', '4863', '4859', '4858', 
                                       '4857', '4590', '4564', '4562', '4545', '4543', '4536', '4391','4341', '4267', '4241', '4092', '4086', 
                                       '4067', '4044', '4043', '1718', '1709', '1498', '1480', '1472', '1438', '1433', '1344', '1322', 
                                       '1317', '1314', '1313', '1311', '1308', '0993', '0856', '0853', '0726', '0721', '0715', '0713', 
                                       '0621', '0620', '0517', '0347', '0342', '0328', '0319', '0314', '0313', '0302') THEN 'UKG'
    ELSE 'NA' END AS UKFlag,
    CCC_STRATEGY,
    SUM (USD_FX) AS delta
FROM cdwuser.U_EXP_TRENDS a
WHERE
 cob_date IN ('2018-02-28','2018-02-21')
    AND CURRENCY_CODE = 'EUR' AND
    (CCC_BUSINESS_AREA IN ('CPM', 'CPM TRADING (MPE)', 'CREDIT', 'COMMODS FINANCING', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR
     CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES')) AND
    ccc_product_line NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE')       AND CCC_DIVISION NOT IN ('FID DVA', 'FIC DVA')  AND CCC_STRATEGY NOT IN ('MS DVA STR NOTES IED') 
GROUP BY
    COB_DATE,
    PRODUCT_TYPE_CODE,
    PRODUCT_SUB_TYPE_CODE,
    CCC_PL_REPORTING_REGION,
    CCC_TAPS_COMPANY,
    COUNTRY_CD_OF_RISK,
    COUNTRY_OF_RISK_REGION,
    CCC_DIVISION,
    CCC_BUSINESS_AREA,
    CCC_PRODUCT_LINE,
    TERM_NEW, TERM_BUCKET,
    CVA_FL,
    HEDGE_STRATEGY,
    CURRENCY_CODE,case when a.PRODUCT_TYPE_CODE in ('AGN', 'BOND', 'BONDFUT', 'BONDFUTOPT', 'GOVTBONDOPT', 'GOVTBONDOPTIL', 'GVTBOND','GVTBONDIL', 'GVTFRN', 'TRRSWAP', 'TRS - GVTBOND', 'TRS - GVTBONDIL') then 'BONDS' else 'SWAPS' end ,
    CASE WHEN ccc_taps_company IN ('7800', '4068', '8962', '8961', '8959', '8941', '8790', '8789', '8772', '8757', '8627', '8564', 
                                       '8537', '8524', '8441', '8292', '8290', '8284', '8275', '8253', '8237', '8179', '8174', '7716', 
                                       '7705', '7704', '7458', '7435', '7416', '7281', '7280', '7043', '7016', '6899', '6893', '6838', 
                                       '6837', '6590', '6589', '6384', '6383', '6376', '6374', '6367', '6325', '6316', '6262', '6158', 
                                       '6157', '6120', '6114', '6036', '5869', '5856', '5656', '5614', '5357', '5310', '5274', '5254', 
                                       '5181', '5180', '5148', '5121', '5104', '5103', '4884', '4880', '4876', '4863', '4859', '4858', 
                                       '4857', '4590', '4564', '4562', '4545', '4543', '4536', '4391', '4341', '4267', '4241', '4092', '4086', 
                                       '4067', '4044', '4043', '1718', '1709', '1498', '1480', '1472', '1438', '1433', '1344', '1322', 
                                       '1317', '1314', '1313', '1311', '1308', '0993', '0856', '0853', '0726', '0721', '0715', '0713', 
                                       '0621', '0620', '0517', '0347', '0342', '0328', '0319', '0314', '0313', '0302') THEN 'UKG'
    ELSE 'NA' END,
    CCC_STRATEGY