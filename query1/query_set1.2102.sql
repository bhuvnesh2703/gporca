SELECT COB_DATE, case when LIMIT_ID IN ('7129','7103','9518','7308') AND COB_DATE <= '11/02/2016' THEN '7350' WHEN LIMIT_ID IN ('7718') AND COB_DATE <= '11/02/2016' THEN '285' when LIMIT_ID IN ('7131','7104','9520','7309') AND COB_DATE <= '11/02/2016' THEN '4' when LIMIT_ID IN ('7130','7107','9519','7293') AND COB_DATE <= '11/02/2016' THEN '62' when LIMIT_ID IN ('7134','7102','9521','7291') AND COB_DATE <= '11/02/2016' THEN '921' when LIMIT_ID IN ('7135','7294','7423') AND COB_DATE <= '11/02/2016' THEN '922' when LIMIT_ID IN ('9522') AND COB_DATE <= '11/02/2016' THEN '9499' WHEN LIMIT_ID IN ('7133','7106','9517','7295') AND COB_DATE <= '11/02/2016' THEN '854' when LIMIT_ID IN ('9935','8744') AND COB_DATE <= '11/02/2016' THEN '7418' when LIMIT_ID IN ('9936') AND COB_DATE <= '11/02/2016' THEN '7419' when LIMIT_ID IN ('8745') AND COB_DATE <= '11/02/2016' THEN '7430' When LIMIT_ID IN ('8916') AND COB_DATE <= '11/02/2016' THEN '8746' When LIMIT_ID IN ('10035') AND COB_DATE <= '11/09/2016' THEN '913' When LIMIT_ID IN ('10036') AND COB_DATE <= '11/09/2016' THEN '916' When LIMIT_ID IN ('10039') AND COB_DATE <= '11/02/2016' THEN '6173' ELSE LIMIT_ID END AS LIMIT_ID, LIMIT_NAME, LIMIT_APPLIED_TO, PARENT_DESCRIPTION, CHILD_DESCRIPTION, LIMIT_VALUE AS LIMIT_AMOUNT, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END AS LIMIT_AMOUNT_WITH_TEMP, sum(CASE WHEN LIMIT_APPLIED_TO <> 'Net Total' AND SIGN(LIMIT_VALUE) <> SIGN(RISK_VALUE_OVERRIDE) THEN 0 ELSE RISK_VALUE_OVERRIDE END) AS RISK_VALUE_OVERRIDE FROM CDWUSER.U_FLOW_LIMITS WHERE COB_DATE <='02/28/2018' AND COB_DATE >= '02/01/2017' AND LIMIT_ID IN ( '7267', '7269', '7261', '7268', '7262', '9666', '9667', '7266', '10013', '7265', '10010', '10011', '10009', '10012', '7717','7132','7105','9523','7136','7308','7309','7293','7292','7295','7291','7294', '2036','8746','5603','3','6054','529','1003','8840','10335','10017','8748','5182','9976','10336','10039','6173','10035','10036','913','916','10037','7109','2260','7105', '7129','7103','9518','7350','7718','285','7131','7104','9520','4','7130','7107','9519','62','7134','7102','9521','921','7135','922','9522','9499', '7133','7106','9517','854','9935','8744','7418','9936','7419' ,'8745','7430','8916','8746','8005','7423', '249','1239','90', '92','93', '10185','2815','8051','2775','4851','8837', '8129','8130','8117','8118','9849','9847','9839','9848','9853','9852','9850','9851' ) AND (PARENT_DESCRIPTION LIKE '%MSCO%' OR PARENT_DESCRIPTION LIKE '%MSCS%' OR PARENT_DESCRIPTION LIKE '%MSCAP%' OR PARENT_DESCRIPTION LIKE '%MORGAN STANLEY DERIVATIVE PRODUCTS%' OR PARENT_DESCRIPTION LIKE '%MSCGI%') GROUP BY COB_DATE, LIMIT_ID, LIMIT_NAME, LIMIT_APPLIED_TO, PARENT_DESCRIPTION, CHILD_DESCRIPTION, LIMIT_VALUE, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END UNION SELECT A.COB_DATE, CASE WHEN A.LIMIT_ID IN ('8919','8844') AND A.COB_DATE <= '11/02/2016' THEN '6302' WHEN A.LIMIT_ID IN ('8920','8845') AND A.COB_DATE <= '11/02/2016' THEN '6303' WHEN A.LIMIT_ID IN ('8918','8843') AND A.COB_DATE <= '11/02/2016' THEN '6304' WHEN A.LIMIT_ID IN ('8921','8846') AND A.COB_DATE <= '11/02/2016' THEN '6305' when LIMIT_ID IN ('8743') AND COB_DATE <= '11/02/2016' THEN '7445' ELSE A.LIMIT_ID END AS LIMIT_ID, A.LIMIT_NAME, A.LIMIT_APPLIED_TO, A.PARENT_DESCRIPTION, A.CHILD_DESCRIPTION, A.LIMIT_VALUE AS LIMIT_AMOUNT, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END AS LIMIT_AMOUNT_WITH_TEMP, CASE WHEN MIN(RISK_VALUE_OVERRIDE) >= 0 THEN 0 ELSE MIN(RISK_VALUE_OVERRIDE) END AS RISK_VALUE_OVERRIDE FROM CDWUSER.U_FLOW_LIMITS A WHERE COB_DATE <='02/28/2018' AND COB_DATE >= '02/01/2017' AND A.LIMIT_ID IN ( '8843','8844','8845','8846','8918','8919','8920','8921','6302','6303','6304','6305','8743','7445','10232' ) AND A.AGGREGATE_BY_VALUE not in ('N/A') AND (PARENT_DESCRIPTION LIKE '%MSCO%' OR PARENT_DESCRIPTION LIKE '%MSCS%' OR PARENT_DESCRIPTION LIKE '%MSCAP%' OR PARENT_DESCRIPTION LIKE '%MORGAN STANLEY DERIVATIVE PRODUCTS%' OR PARENT_DESCRIPTION LIKE '%MSCGI%') GROUP BY A.COB_DATE, A.LIMIT_ID, A.LIMIT_NAME, A.LIMIT_APPLIED_TO, A.PARENT_DESCRIPTION, A.CHILD_DESCRIPTION, LIMIT_VALUE, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END UNION SELECT A.COB_DATE, CASE WHEN A.LIMIT_ID IN ('8923','8848') AND A.COB_DATE <= '11/02/2016' THEN '6241' WHEN A.LIMIT_ID IN ('8922','8847') AND A.COB_DATE <= '11/02/2016' THEN '6240' ELSE A.LIMIT_ID END AS LIMIT_ID, A.LIMIT_NAME, A.LIMIT_APPLIED_TO, A.PARENT_DESCRIPTION, A.CHILD_DESCRIPTION, A.LIMIT_VALUE AS LIMIT_AMOUNT, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END AS LIMIT_AMOUNT_WITH_TEMP, CASE WHEN MAX(RISK_VALUE_OVERRIDE) <= 0 THEN 0 ELSE MAX(RISK_VALUE_OVERRIDE) END AS RISK_VALUE_OVERRIDE FROM CDWUSER.U_FLOW_LIMITS A WHERE COB_DATE <='02/28/2018' AND COB_DATE >= '02/01/2017' AND A.LIMIT_ID IN ( '8922','8923','8847','8848','6241','6240' ) and a.AGGREGATE_BY_VALUE not in ('N/A') AND (PARENT_DESCRIPTION LIKE '%MSCO%' OR PARENT_DESCRIPTION LIKE '%MSCS%' OR PARENT_DESCRIPTION LIKE '%MSCAP%' OR PARENT_DESCRIPTION LIKE '%MORGAN STANLEY DERIVATIVE PRODUCTS%' OR PARENT_DESCRIPTION LIKE '%MSCGI%') GROUP BY A.COB_DATE, A.LIMIT_ID, A.LIMIT_NAME, A.LIMIT_APPLIED_TO, A.PARENT_DESCRIPTION, A.CHILD_DESCRIPTION, LIMIT_VALUE, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END UNION select A.COB_DATE, CASE WHEN A.LIMIT_ID IN ('8842') AND A.COB_DATE <= '11/02/2016' THEN '2055' ELSE A.LIMIT_ID END AS LIMIT_ID, A.LIMIT_NAME, A.LIMIT_APPLIED_TO, A.PARENT_DESCRIPTION, A.CHILD_DESCRIPTION, A.LIMIT_VALUE AS LIMIT_AMOUNT, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END AS LIMIT_AMOUNT_WITH_TEMP, CASE WHEN (abs(MAX(A.RISK_VALUE_OVERRIDE)) > abs(MIN(A.RISK_VALUE_OVERRIDE))) THEN MAX(A.RISK_VALUE_OVERRIDE) ELSE MIN(A.RISK_VALUE_OVERRIDE) END AS RISK_VALUE_OVERRIDE FROM CDWUSER.U_FLOW_LIMITS A WHERE COB_DATE <='02/28/2018' AND COB_DATE >= '02/01/2017' AND A.LIMIT_ID IN ( '8842','10189','2055','6155','5147','10418','10472','10271','10986' ) and a.AGGREGATE_BY_VALUE not in ('N/A') AND (PARENT_DESCRIPTION LIKE '%MSCO%' OR PARENT_DESCRIPTION LIKE '%MSCS%' OR PARENT_DESCRIPTION LIKE '%MSCAP%' OR PARENT_DESCRIPTION LIKE '%MORGAN STANLEY DERIVATIVE PRODUCTS%' OR PARENT_DESCRIPTION LIKE '%MSCGI%') GROUP BY A.COB_DATE, A.LIMIT_ID, A.LIMIT_NAME, A.LIMIT_APPLIED_TO, A.PARENT_DESCRIPTION, A.CHILD_DESCRIPTION, A.LIMIT_VALUE, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END union select A.COB_DATE, a.LIMIT_ID, A.LIMIT_NAME, A.LIMIT_APPLIED_TO, A.PARENT_DESCRIPTION, A.CHILD_DESCRIPTION, A.LIMIT_VALUE AS LIMIT_AMOUNT, CASE WHEN TEMP_LIMIT_VALUE IS NULL THEN LIMIT_VALUE WHEN (TEMP_LIMIT_VALUE IS NOT NULL AND TEMP_LIMIT_EXPIRATION_DATE > COB_DATE) THEN TEMP_LIMIT_VALUE ELSE LIMIT_VALUE END AS LIMIT_AMOUNT_WITH_TEMP, CASE WHEN A.LIMIT_APPLIED_TO <> 'Net Total' AND SIGN(A.LIMIT_VALUE) <> SIGN(A.RISK_VALUE_OVERRIDE) THEN 0 ELSE A.RISK_VALUE_OVERRIDE END AS RISK_VALUE_OVERRIDE FROM CDWUSER.U_FLOW_LIMITS A WHERE COB_DATE <='02/28/2018' AND COB_DATE >= '02/01/2017' AND A.LIMIT_ID IN ( '8837','8838','8839','8840','8841','8748','9189','10335' ) and a.AGGREGATE_BY_VALUE not in ('N/A') AND (PARENT_DESCRIPTION LIKE '%MSCO%' OR PARENT_DESCRIPTION LIKE '%MSCS%' OR PARENT_DESCRIPTION LIKE '%MSCAP%' OR PARENT_DESCRIPTION LIKE '%MORGAN STANLEY DERIVATIVE PRODUCTS%' OR PARENT_DESCRIPTION LIKE '%MSCGI%')