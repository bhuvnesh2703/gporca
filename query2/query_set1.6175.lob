WITH
    IED AS (SELECT 
                COB_DATE,
                PROCESS_ID,
                POSITION_ID,
                
    Sum(USD_EQ_DISC_RISK) as Dividends,
    Sum(USD_EQ_SKEW*(1/SQRT(GREATEST(0.0833,TERM_OF_MEASURE/365)))) as Skew
    
            FROM cdwuser.U_EQ_MSR 
            WHERE 
  cob_date IN ('2018-02-28','2018-02-21') 
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
                               
                           ABS (SUM (PRODUCT_WEIGHT_DECOMP)) AS WEIGHT
                       FROM cdwuser.U_DECOMP_MSR d
                       WHERE
  cob_date IN ('2018-02-28','2018-02-21') 
                           
                           AND LE_GROUP = 'UK'
                           AND DIVISION = 'IED' 
                           AND d.CCC_BANKING_TRADING <> 'BANKING'
                       GROUP BY
                           d.COB_DATE,
                           PROCESS_ID,
                           POSITION_ID,
                           ISSUER_COUNTRY_CODE_DECOMP

                                 
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
   
    SUM(Dividends) AS Dividends,
    SUM(Skew) AS Skew
    
FROM
    (
        SELECT
            i.COB_DATE,
            COUNTRY,
            
    (Dividends * WEIGHT) AS Dividends,
    (Skew * WEIGHT) AS Skew
    
        FROM
            IED i
            INNER JOIN
            Decomp d
            ON
                i.PROCESS_ID = d.PROCESS_ID AND
                i.POSITION_ID = d.POSITION_ID AND
                i.COB_DATE = d.COB_DATE
  WHERE (COUNTRY in ('ESP')     )
  )
    M
GROUP BY
    COUNTRY,
    COB_DATE