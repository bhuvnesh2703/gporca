WITH IED AS ( SELECT COB_DATE, PROCESS_ID ,POSITION_ID ,CCC_PL_REPORTING_REGION ,LE_GROUP ,sum(D50RAW) AS D50RAW ,Sum(D30RAW) AS D30RAW ,sum(D20RAW) AS D20RAW ,sum(D10RAW) AS D10RAW ,sum(D5RAW) AS D5RAW ,sum(P5RAW) AS P5RAW ,sum(P10RAW) AS P10RAW ,sum(P20RAW) AS P20RAW FROM ( SELECT e.COB_DATE, PROCESS_ID ,POSITION_ID ,e.CCC_PL_REPORTING_REGION ,e.LE_GROUP ,sum(e.SLIDE_EQ_MIN_50_USD) AS D50RAW ,sum(e.SLIDE_EQ_MIN_30_USD) AS D30RAW ,sum(e.SLIDE_EQ_MIN_20_USD) AS D20RAW ,sum(e.SLIDE_EQ_MIN_10_USD) AS D10RAW ,sum(e.SLIDE_EQ_MIN_05_USD) AS D5RAW ,sum(e.SLIDE_EQ_PLS_05_USD) AS P5RAW ,sum(e.SLIDE_EQ_PLS_10_USD) AS P10RAW ,sum(e.SLIDE_EQ_PLS_20_USD) AS P20RAW FROM cdwuser.U_EQ_MSR e WHERE e.COB_DATE in ( '2018-02-28') AND e.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND e.CCC_BANKING_TRADING <> 'BANKING' AND SILO_SRC = 'IED' GROUP BY e.PROCESS_ID ,POSITION_ID, e.COB_DATE ,e.CCC_PL_REPORTING_REGION ,e.LE_GROUP ) a GROUP BY PROCESS_ID ,POSITION_ID, COB_DATE ,CCC_PL_REPORTING_REGION ,LE_GROUP ) ,CountryWeights AS ( SELECT d.COB_DATE ,PROCESS_ID ,POSITION_ID ,CCC_PL_REPORTING_REGION ,LE_GROUP ,ISSUER_COUNTRY_CODE_DECOMP AS COUNTRY ,abs(sum(PRODUCT_WEIGHT_DECOMP)) AS WEIGHT FROM cdwuser.U_DECOMP_MSR d WHERE d.COB_DATE in ( '2018-02-28') AND d.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND d.CCC_BANKING_TRADING <> 'BANKING' AND SILO_SRC = 'IED' GROUP BY d.COB_DATE ,PROCESS_ID ,POSITION_ID ,CCC_PL_REPORTING_REGION ,LE_GROUP ,ISSUER_COUNTRY_CODE_DECOMP HAVING sum(PRODUCT_WEIGHT_DECOMP) <> 0 ) ,GrossWeights AS ( SELECT x.COB_DATE ,PROCESS_ID ,POSITION_ID ,CCC_PL_REPORTING_REGION ,LE_GROUP ,sum(abs(WEIGHT)) AS GROSS_WEIGHT FROM CountryWeights x GROUP BY x.COB_DATE ,PROCESS_ID ,POSITION_ID ,CCC_PL_REPORTING_REGION ,LE_GROUP ) ,Decomp AS ( SELECT w.COB_DATE ,w.CCC_PL_REPORTING_REGION ,w.LE_GROUP ,w.Process_ID ,w.Position_id ,COUNTRY ,abs(WEIGHT / GROSS_WEIGHT) AS WEIGHT FROM CountryWeights w INNER JOIN GrossWeights g ON ( w.cob_date = g.cob_date AND w.process_id = g.process_id AND w.position_id = g.position_id AND w.CCC_PL_REPORTING_REGION=g.CCC_PL_REPORTING_REGION AND w.LE_GROUP=g.LE_GROUP) ) SELECT VIEW, COB_DATE ,CCC_PL_REPORTING_REGION ,LE_GROUP ,case when Country in ('AUT','BEL','BGR','CHE','CZE','DEU','DNK','ESP','EST','FIN','FRA','GEO','GGY','GRC','HRV','HUN','IRL','ISL','ITA','JEY','LTU','LUX','NLD','NOR','POL','PRT','ROU','RUS','SVK','SVN','SWE') Then 'Europe' when Country in ('ARE','BGD','BHR','CHN','HKG','IDN','IND','ISR','JOR','JPN','KAZ','KOR','KWT','LBN','LKA','MYS','OMN','PAK','PHL','QAT','SAU','SGP','THA','TUR','TWN','VNM') Then 'Asia' when Country in ('GBR') Then 'UK' when Country in ('USA') Then 'USA' else 'Other' end as Country ,SUM( D50RAW )/1000 AS D50RAW ,SUM( D30RAW )/1000 AS D30RAW ,SUM( D20RAW )/1000 AS D20RAW ,SUM( D10RAW )/1000 AS D10RAW ,SUM( D5RAW )/1000 AS D5RAW ,SUM( P5RAW )/1000 AS P5RAW ,SUM( P10RAW )/1000 AS P10RAW ,SUM( P20RAW )/1000 AS P20RAW ,0 as Base FROM ( SELECT 'Decomp' AS VIEW, i.COB_DATE ,COUNTRY ,i.CCC_PL_REPORTING_REGION ,i.LE_GROUP ,(D50RAW * WEIGHT) AS D50RAW ,(D30RAW * WEIGHT) AS D30RAW ,(D20RAW * WEIGHT) AS D20RAW ,(D10RAW * WEIGHT) AS D10RAW ,(D5RAW * WEIGHT) AS D5RAW ,(P5RAW * WEIGHT) AS P5RAW ,(P10RAW * WEIGHT) AS P10RAW ,(P20RAW * WEIGHT) AS P20RAW FROM IED i INNER JOIN Decomp d ON ( i.PROCESS_ID = d.PROCESS_ID AND i.POSITION_ID = d.POSITION_ID and i.COB_DATE = d.COB_DATE AND i.CCC_PL_REPORTING_REGION=d.CCC_PL_REPORTING_REGION AND i.LE_GROUP=d.LE_GROUP ) ) a GROUP BY VIEW ,Country ,COB_DATE ,CCC_PL_REPORTING_REGION ,LE_GROUP ,case when Country in ('AUT','BEL','BGR','CHE','CZE','DEU','DNK','ESP','EST','FIN','FRA','GEO','GGY','GRC','HRV','HUN','IRL','ISL','ITA','JEY','LTU','LUX','NLD','NOR','POL','PRT','ROU','RUS','SVK','SVN','SWE') Then 'Europe' when Country in ('ARE','BGD','BHR','CHN','HKG','IDN','IND','ISR','JOR','JPN','KAZ','KOR','KWT','LBN','LKA','MYS','OMN','PAK','PHL','QAT','SAU','SGP','THA','TUR','TWN','VNM') Then 'Asia' when Country in ('GBR') Then 'UK' when Country in ('USA') Then 'USA' else 'Other' end