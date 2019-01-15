WITH IED AS ( SELECT COB_DATE, PROCESS_ID ,POSITION_ID ,sum(D50RAW) AS D50RAW ,Sum(D30RAW) AS D30RAW ,sum(D20RAW) AS D20RAW ,sum(D10RAW) AS D10RAW ,sum(D5RAW) AS D5RAW ,sum(P5RAW) AS P5RAW ,sum(P10RAW) AS P10RAW ,sum(P20RAW) AS P20RAW from ( SELECT e.COB_DATE, PROCESS_ID ,POSITION_ID ,sum(e.SLIDE_EQ_MIN_50_USD) AS D50RAW ,sum(e.SLIDE_EQ_MIN_30_USD) AS D30RAW ,sum(e.SLIDE_EQ_MIN_20_USD) AS D20RAW ,sum(e.SLIDE_EQ_MIN_10_USD) AS D10RAW ,sum(e.SLIDE_EQ_MIN_05_USD) AS D5RAW ,sum(e.SLIDE_EQ_PLS_05_USD) AS P5RAW ,sum(e.SLIDE_EQ_PLS_10_USD) AS P10RAW ,sum(e.SLIDE_EQ_PLS_20_USD) AS P20RAW FROM CDWUSER.U_EQ_MSR e WHERE (e.COB_DATE = '2017-08-10' or e.COB_DATE = '2017-08-10') AND e.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND e.CCC_BANKING_TRADING <> 'BANKING' AND SILO_SRC = 'IED' AND e.LE_GROUP = 'UK' GROUP BY e.PROCESS_ID ,POSITION_ID, e.COB_DATE )sub_qry GROUP BY PROCESS_ID ,POSITION_ID, COB_DATE ) ,CountryWeights AS ( SELECT d.COB_DATE ,PROCESS_ID ,POSITION_ID ,FID1_INDUSTRY_NAME_LEVEL5 AS INDUSTRY ,FID1_INDUSTRY_NAME_LEVEL4 AS INDUSTRY2 ,abs(sum(PRODUCT_WEIGHT_DECOMP)) AS WEIGHT FROM CDWUSER.U_DECOMP_MSR d WHERE (d.COB_DATE = '2017-08-10' or d.COB_DATE = '2017-08-10') AND d.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND d.CCC_BANKING_TRADING <> 'BANKING' AND SILO_SRC = 'IED' AND d.LE_GROUP = 'UK' GROUP BY d.COB_DATE ,PROCESS_ID ,POSITION_ID ,FID1_INDUSTRY_NAME_LEVEL5, FID1_INDUSTRY_NAME_LEVEL4 HAVING sum(PRODUCT_WEIGHT_DECOMP) <> 0 ) ,GrossWeights AS ( SELECT x.COB_DATE ,PROCESS_ID ,POSITION_ID ,sum(abs(WEIGHT)) AS GROSS_WEIGHT FROM CountryWeights x GROUP BY x.COB_DATE ,PROCESS_ID ,POSITION_ID ) ,Decomp AS ( SELECT w.COB_DATE ,w.Process_ID ,w.Position_id , w.INDUSTRY2 ,abs(WEIGHT / GROSS_WEIGHT) AS WEIGHT FROM CountryWeights w INNER JOIN GrossWeights g ON ( w.cob_date = g.cob_date AND w.process_id = g.process_id AND w.position_id = g.position_id ) ) SELECT VIEW, COB_DATE , INDUSTRY2 ,sum(D50RAW) AS D50RAW ,sum(D30RAW) AS D30RAW ,sum(D20RAW) AS D20RAW ,sum(D10RAW) AS D10RAW ,sum(D5RAW) AS D5RAW ,sum(P5RAW) AS P5RAW ,sum(P10RAW) AS P10RAW ,sum(P20RAW) AS P20RAW from ( SELECT 'Decomp' AS VIEW, i.COB_DATE , INDUSTRY2 ,(D50RAW * WEIGHT) AS D50RAW ,(D30RAW * WEIGHT) AS D30RAW ,(D20RAW * WEIGHT) AS D20RAW ,(D10RAW * WEIGHT) AS D10RAW ,(D5RAW * WEIGHT) AS D5RAW ,(P5RAW * WEIGHT) AS P5RAW ,(P10RAW * WEIGHT) AS P10RAW ,(P20RAW * WEIGHT) AS P20RAW FROM IED i INNER JOIN Decomp d ON ( i.PROCESS_ID = d.PROCESS_ID AND i.POSITION_ID = d.POSITION_ID and i.COB_DATE = d.COB_DATE ) )sub_qry GROUP BY VIEW ,INDUSTRY2 ,COB_DATE