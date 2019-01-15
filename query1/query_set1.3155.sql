WITH
    IED AS (SELECT
                COB_DATE,
                PROCESS_ID,
                POSITION_ID,
                                SUM (CASE WHEN scenario_name = 'S-50%/V+.1' THEN POS_TV_MINUS_BASE END) AS D50V01,
                                SUM (CASE WHEN scenario_name = 'S-20%/V+.05' THEN POS_TV_MINUS_BASE END) AS D20V05,
SUM (CASE WHEN scenario_name = 'S-50%' THEN POS_TV_MINUS_BASE END) AS D50

            FROM cdwuser.U_IED_SURFACE
            WHERE 
 cob_date IN ('2018-02-28','2018-02-27')
            GROUP BY
                PROCESS_ID,
                POSITION_ID,
                COB_DATE),
    /*Aggregate the weights on country issuer level*/
    CountryWeights AS (SELECT
                           d.COB_DATE,
                           PROCESS_ID,
                           POSITION_ID,
                           ISSUER_COUNTRY_CODE_DECOMP AS COUNTRY,
                           CASE WHEN FID1_INDUSTRY_NAME_LEVEL2 IN ('CONSUMER DISCRETIONARY') THEN 'CONSUMER DISCRETIONARY' WHEN 
                               FID1_INDUSTRY_NAME_LEVEL2 IN ('CONSUMER STAPLES') THEN 'CONSUMER STAPLES' WHEN FID1_INDUSTRY_NAME_LEVEL2 
                               IN ('ENERGY') THEN 'ENERGY' WHEN FID1_INDUSTRY_NAME_LEVEL2 IN ('FINANCIALS') THEN 'FINANCIALS' WHEN 
                               FID1_INDUSTRY_NAME_LEVEL2 IN ('HEALTH CARE') THEN 'HEALTH CARE' WHEN FID1_INDUSTRY_NAME_LEVEL2 IN (
                                                                                                                                      'INDUSTRIALS'
                               ) THEN 'INDUSTRIALS' WHEN FID1_INDUSTRY_NAME_LEVEL2 IN ('INFORMATION TECHNOLOGY') THEN 
                               'INFORMATION TECHNOLOGY' WHEN FID1_INDUSTRY_NAME_LEVEL2 IN ('MATERIALS') THEN 'MATERIALS' WHEN 
                               FID1_INDUSTRY_NAME_LEVEL2 IN ('TELECOMMUNICATION SERVICES') THEN 'TELECOMMUNICATION SERVICES' WHEN 
                               FID1_INDUSTRY_NAME_LEVEL2 IN ('UTILITIES') THEN 'UTILITIES'
                           ELSE 'OTHERS' END AS INDUSTRY,
                           ABS (SUM (PRODUCT_WEIGHT_DECOMP)) AS WEIGHT
                       FROM cdwuser.U_DECOMP_MSR d
                       WHERE
 cob_date IN ('2018-02-28','2018-02-27')
AND d.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION'
AND d.CCC_STRATEGY NOT IN ('MS DVA STR NOTES IED') 

AND d.CCC_BANKING_TRADING <> 'BANKING'
                       GROUP BY
                           d.COB_DATE,
                           PROCESS_ID,
                           POSITION_ID,
                           ISSUER_COUNTRY_CODE_DECOMP,
                           CASE WHEN FID1_INDUSTRY_NAME_LEVEL2 IN ('CONSUMER DISCRETIONARY') THEN 'CONSUMER DISCRETIONARY' WHEN 
                               FID1_INDUSTRY_NAME_LEVEL2 IN ('CONSUMER STAPLES') THEN 'CONSUMER STAPLES' WHEN FID1_INDUSTRY_NAME_LEVEL2 
                               IN ('ENERGY') THEN 'ENERGY' WHEN FID1_INDUSTRY_NAME_LEVEL2 IN ('FINANCIALS') THEN 'FINANCIALS' WHEN 
                               FID1_INDUSTRY_NAME_LEVEL2 IN ('HEALTH CARE') THEN 'HEALTH CARE' WHEN FID1_INDUSTRY_NAME_LEVEL2 IN (
                                                                                                                                      'INDUSTRIALS'
                               ) THEN 'INDUSTRIALS' WHEN FID1_INDUSTRY_NAME_LEVEL2 IN ('INFORMATION TECHNOLOGY') THEN 
                               'INFORMATION TECHNOLOGY' WHEN FID1_INDUSTRY_NAME_LEVEL2 IN ('MATERIALS') THEN 'MATERIALS' WHEN 
                               FID1_INDUSTRY_NAME_LEVEL2 IN ('TELECOMMUNICATION SERVICES') THEN 'TELECOMMUNICATION SERVICES' WHEN 
                               FID1_INDUSTRY_NAME_LEVEL2 IN ('UTILITIES') THEN 'UTILITIES'
                           ELSE 'OTHERS' END
                       HAVING SUM (PRODUCT_WEIGHT_DECOMP) <> 0),
    /*Total weights*/
    GrossWeights AS (SELECT
                         x.COB_DATE,
                         PROCESS_ID,
                         POSITION_ID,
                         SUM (ABS (WEIGHT)) AS GROSS_WEIGHT
                     FROM CountryWeights x
                     GROUP BY
                         x.COB_DATE,
                         PROCESS_ID,
                         POSITION_ID),
    /*Calculated weights on issuer country code level*/
    Decomp AS (SELECT
                   w.COB_DATE,
                   w.Process_ID,
                   w.Position_id,
                   COUNTRY,
                   INDUSTRY,
                   ABS (WEIGHT / GROSS_WEIGHT) AS WEIGHT
               FROM
                   CountryWeights w
                   INNER JOIN
                   GrossWeights g
                   ON (w.cob_date = g.cob_date AND
                       w.process_id = g.process_id AND
                       w.position_id = g.position_id))
/*Final query with country categorization and join to modular data via rowid, using the calculated weights to split scenario PnL results*/
SELECT
    COB_DATE,
    COUNTRY,
    INDUSTRY,
    SUM (D50V01) AS D50V01,
    SUM (D20V05) AS D20V05,
SUM (D50) as D50
FROM
    (
        SELECT
            i.COB_DATE,
            COUNTRY,
            INDUSTRY,
            (D50V01 * WEIGHT) AS D50V01,
            (D20V05 * WEIGHT) AS D20V05,
            (D50 * WEIGHT) AS D50
        FROM
            IED i
            INNER JOIN
            Decomp d
            ON
                i.PROCESS_ID = d.PROCESS_ID AND
                i.POSITION_ID = d.POSITION_ID AND
                i.COB_DATE = d.COB_DATE
        WHERE COUNTRY in ('RUS')
    )
    M
GROUP BY
    COUNTRY,
    INDUSTRY,
    COB_DATE