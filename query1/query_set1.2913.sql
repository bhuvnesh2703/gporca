SELECT     D.MAPPING,     D.CURRENCY_PAIR,     D.SPOT,     D.VOL,     SUM         (         VOL200 + VOL100 +          CAST(1 AS FLOAT) / 2 * VOL60 +          CAST(1 AS FLOAT) / 2 * VOL40 +         VOL0 + VOLM50         )     AS SCENARIO_PNL FROM     (         SELECT         CCY1|| '/' || REGION AS MAPPING,         CURRENCY_PAIR,         SPOT,         CASE             WHEN                 VOL IN ('40%','60%')                 THEN '50%'             ELSE                 VOL         END         AS VOL,         CASE             WHEN                 VOL IN ('200%')                  THEN COALESCE (SCENARIO_PNL,0)             ELSE 0         END          AS VOL200,         CASE             WHEN                 VOL IN ('100%')                  THEN COALESCE (SCENARIO_PNL,0)             ELSE 0         END          AS VOL100,         CASE             WHEN                 VOL IN ('40%')                  THEN COALESCE (SCENARIO_PNL,0)             ELSE 0         END          AS VOL40,         CASE             WHEN                 VOL IN ('60%')                  THEN COALESCE (SCENARIO_PNL,0)             ELSE 0         END          AS VOL60,         CASE             WHEN                 VOL IN ('0%')                  THEN COALESCE (SCENARIO_PNL,0)             ELSE 0         END          AS VOL0,         CASE             WHEN                 VOL IN ('-50%')                  THEN COALESCE (SCENARIO_PNL,0)             ELSE 0         END          AS VOLM50     FROM         (         SELECT             CURRENCY_PAIR,             CASE                 WHEN                     SPOT IN ('20%','35%')                     THEN '-20%'                 WHEN                     SPOT IN ('10%','15%')                     THEN '-10%'                 WHEN                     SPOT IN ('-10%','-5%')                     THEN '10%'                    WHEN                     SPOT IN ('-20%','-15%')                      THEN '20%'                    WHEN                         SPOT IN ('0%')                      THEN '0%'               END             AS SPOT,             VOL,             SUM                 (                 CAST(1 AS FLOAT) / 3 * SPOT35 + CAST(2 AS FLOAT) / 3 * SPOT20 +                  CAST(2 AS FLOAT) / 9 * SPOT15 + CAST(7 AS FLOAT) / 9 * SPOT10 +                  SPOT0 + CAST(2 AS FLOAT) / 11 * SPOTM5 + CAST(9 AS FLOAT) / 11 * SPOTM10 +                  CAST(2 AS FLOAT) / 3 * SPOTM15 + CAST(1 AS FLOAT) / 3 * SPOTM20                 )             AS SCENARIO_PNL,             SUBSTR(CURRENCY_PAIR,1,3) AS CCY1,             CASE                 WHEN                     SUBSTR(CURRENCY_PAIR,4) IN                          (                         'BDT','BND','BTN','CNY','FJD','IDR','INR','KGS','KHR','KPW',                         'KRW','LAK','LKR','MMK','MNT','MOP','MYR','NPR','PGK','PHP',                         'PKR','SBD','SGD','THB','TJS','TMM','TOP','TWD','UZS','VND',                         'VUV','WST','HKD'                         ) THEN 'ASIA'                 WHEN                               SUBSTR(CURRENCY_PAIR,4) IN                          (                         'AMD','AZM','AZN','BAM','BGN','BYR','CSD','CZK','EEK','GEL','HRK',                         'HUF','ISK','KZT','LTL','LVL','MDL','MKD','PLN','ROL','RON','RSD',                         'RUB','SIT','SKK','TRL','TRY','UAH','AOA','BHD','BIF','BWP','CDF',                         'CVE','DEM','DJF','DZD','EGP','ERN','ETB','GHC','GHS','GMD','GNF',                         'GWP','ILS','IQD','IRR','JOD','KES','KMF','KWD','LBP','LRD','LSL',                         'LYD','MAD','MRO','MUR','MVR','MWK','MZM','NAD','NGN','OMR',                         'RWF','SCR','SDD','SHP','SLL','SOS','STD','SYP','SZL','TND','TZS',                         'UGX','XOF','YER','ZAR','ZMK','ZWD','AED','QAR','SAR'                         ) THEN 'EMEA'                 WHEN                             SUBSTR(CURRENCY_PAIR,4) IN                         (                         'ANG','ARS','AWG','BBD','BMD','BOB','BOV','BRL','BSD','BZD',                         'CLF','CLP','COP','CRC','CUP','DOP','ECU','FKP','GTQ','GYD','HNL',                         'HTG','JMD','KYD','MXN','MXV','NIO','PAB','PEN','PYG','SVC',                         'TTD','UYU','VEB'                         ) THEN 'LATAM'                 WHEN                     SUBSTR(CURRENCY_PAIR,4) IN                         (                         'AUD','CAD','CHF','EUR','GBP','JPY','NOK','NZD','SEK'                         ) THEN 'MAJORS'                 WHEN                     SUBSTR(CURRENCY_PAIR,4) IN                         (                         'DKK'                         ) THEN 'PEGGED'                 WHEN                     SUBSTR(CURRENCY_PAIR,4) IN                         (                         'USD','UBD'                         ) THEN 'USD'                 ELSE                     SUBSTR(CURRENCY_PAIR,4)             END             AS REGION         FROM             (             SELECT                   RAW_PNL,                 SOURCE_SCN_NAME,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot 0%') > 0                         THEN CAST (SUBSTR(SOURCE_SCN_NAME,26,4) AS VARCHAR(4))                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -10%') > 0                         THEN CAST (SUBSTR(SOURCE_SCN_NAME,28,4) AS VARCHAR(4))                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -20%') > 0                         THEN CAST (SUBSTR(SOURCE_SCN_NAME,28,4) AS VARCHAR(4))                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -15%') > 0                         THEN CAST (SUBSTR(SOURCE_SCN_NAME,28,4) AS VARCHAR(4))                     ELSE                         CAST(SUBSTR(SOURCE_SCN_NAME,27,4) AS VARCHAR(4))                 END                 AS VOL,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot 0%') > 0                         THEN CAST(SUBSTR(SOURCE_SCN_NAME,14,2) AS VARCHAR(4))                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -10%') > 0                         THEN CAST(SUBSTR(SOURCE_SCN_NAME,14,4) AS VARCHAR(4))                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -20%') > 0                         THEN CAST(SUBSTR(SOURCE_SCN_NAME,14,4) AS VARCHAR(4))                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -15%') > 0                         THEN CAST(SUBSTR(SOURCE_SCN_NAME,14,4) AS VARCHAR(4))                     ELSE                         CAST (SUBSTR(SOURCE_SCN_NAME,14,3) AS VARCHAR(4))                 END                 AS SPOT,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot 20%') > 0                         THEN COALESCE (RAW_PNL,0)                     ELSE 0                 END                  AS SPOT20,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot 35%') > 0                         THEN COALESCE (RAW_PNL,0)                     ELSE 0                 END                  AS SPOT35,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot 10%') > 0                         THEN COALESCE (RAW_PNL,0)                     ELSE 0                 END                  AS SPOT10,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot 15%') > 0                         THEN COALESCE (RAW_PNL,0)                     ELSE 0                 END                  AS SPOT15,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot 0%') > 0                         THEN COALESCE (RAW_PNL,0)                     ELSE 0                 END                  AS SPOT0,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -10%') > 0                         THEN RAW_PNL                     ELSE 0                 END                  AS SPOTM10,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -5%') > 0                         THEN COALESCE (RAW_PNL,0)                     ELSE 0                 END                  AS SPOTM5,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -20%') > 0                         THEN COALESCE (RAW_PNL,0)                     ELSE 0                 END                  AS SPOTM20,                 CASE                     WHEN                         STRPOS(SOURCE_SCN_NAME, 'spot -15%') > 0                         THEN COALESCE (RAW_PNL,0)                     ELSE 0                 END                  AS SPOTM15,                 CASE                      WHEN                          STRPOS(CURRENCY_PAIR, 'CNH') > 0                         THEN REPLACE(CURRENCY_PAIR, 'CNH', 'CNY')                     WHEN                          STRPOS(CURRENCY_PAIR, 'KRX') > 0                         THEN REPLACE(CURRENCY_PAIR, 'KRX', 'KRW')                     WHEN                          STRPOS(CURRENCY_PAIR, 'RBX') > 0                         THEN REPLACE(CURRENCY_PAIR, 'RBX', 'RUB')                     WHEN                          STRPOS(CURRENCY_PAIR, 'RU1') > 0                         THEN REPLACE(CURRENCY_PAIR, 'RU1', 'RUB')                     WHEN                          CURRENCY_PAIR IN ('UBDBRL','UBDBRX','USDBRL')                         THEN 'USDBRL'                     ELSE CURRENCY_PAIR                 END                  AS CURRENCY_PAIR             FROM                  DWUSER.U_RAW_SCENARIO_PNL A             WHERE      COB_DATE = '2018-01-31' AND     CCC_TAPS_COMPANY IN ('0111') AND                 A.CCC_BUSINESS_AREA = 'FXEM MACRO TRADING' AND                  BOOK NOT IN                      (                     'BASKET', 'BASKET HEDGES', 'CEEMEA MULTICCY', 'CORRELATION SWAP',                      'CORRELATION SWAP 2', 'CORRELATION SWAP 3', 'CORRELATION SWAP 4',                      'DUAL CURRENCY', 'MULTICCY SPEC'                     ) AND                  PRODUCT_TYPE IN ('FXOPT') AND                  BU_RISK_SYSTEM LIKE '%FXOPT%' AND                  PROCESS_ID = '62000' AND                  (                     (                     SOURCE_SCN_NAME LIKE '%spot -20%' OR                        SOURCE_SCN_NAME LIKE '%spot -15%' OR                                SOURCE_SCN_NAME LIKE '%spot -10%' OR                     SOURCE_SCN_NAME LIKE '%spot -5%' OR                             SOURCE_SCN_NAME LIKE '%spot 0%' OR                     SOURCE_SCN_NAME LIKE '%spot 10%' OR                     SOURCE_SCN_NAME LIKE '%spot 15%' OR                     SOURCE_SCN_NAME LIKE '%spot 20%' OR                     SOURCE_SCN_NAME LIKE '%spot 35%'                      ) AND                      SOURCE_SCN_NAME NOT LIKE '%spot 100%' AND                     SOURCE_SCN_NAME NOT LIKE '%spot -50%' AND                     (                     SOURCE_SCN_NAME LIKE '%vol -50%' OR                      SOURCE_SCN_NAME LIKE '%vol 0%' OR                      SOURCE_SCN_NAME LIKE '%vol 40%' OR                      SOURCE_SCN_NAME LIKE '%vol 60%' OR                      SOURCE_SCN_NAME LIKE '%vol 100%' OR                      SOURCE_SCN_NAME LIKE '%vol 200%'                     ) AND                     SOURCE_SCN_NAME NOT LIKE '%vol 1000%' AND                     SOURCE_SCN_NAME NOT LIKE '%vol 500%'                 ) AND                 SOURCE_SCN_NAME LIKE 'USD/EUR%'             ) B         GROUP BY             CURRENCY_PAIR,             CASE                 WHEN                     SPOT IN ('20%','35%')                     THEN '-20%'                 WHEN                     SPOT IN ('10%','15%')                     THEN '-10%'                 WHEN                     SPOT IN ('-10%','-5%')                     THEN '10%'                    WHEN                     SPOT IN ('-20%','-15%')                      THEN '20%'                    WHEN                         SPOT IN ('0%')                      THEN '0%'               END,             VOL,             SUBSTR(CURRENCY_PAIR,1,3),             CASE                 WHEN                     SUBSTR(CURRENCY_PAIR,4) IN                          (                         'BDT','BND','BTN','CNY','FJD','IDR','INR','KGS','KHR','KPW',                         'KRW','LAK','LKR','MMK','MNT','MOP','MYR','NPR','PGK','PHP',                         'PKR','SBD','SGD','THB','TJS','TMM','TOP','TWD','UZS','VND',                         'VUV','WST','HKD'                         ) THEN 'ASIA'                 WHEN                               SUBSTR(CURRENCY_PAIR,4) IN                          (                         'AMD','AZM','AZN','BAM','BGN','BYR','CSD','CZK','EEK','GEL','HRK',                         'HUF','ISK','KZT','LTL','LVL','MDL','MKD','PLN','ROL','RON','RSD',                         'RUB','SIT','SKK','TRL','TRY','UAH','AOA','BHD','BIF','BWP','CDF',                         'CVE','DEM','DJF','DZD','EGP','ERN','ETB','GHC','GHS','GMD','GNF',                         'GWP','ILS','IQD','IRR','JOD','KES','KMF','KWD','LBP','LRD','LSL',                         'LYD','MAD','MRO','MUR','MVR','MWK','MZM','NAD','NGN','OMR',                         'RWF','SCR','SDD','SHP','SLL','SOS','STD','SYP','SZL','TND','TZS',                         'UGX','XOF','YER','ZAR','ZMK','ZWD','AED','QAR','SAR'                         ) THEN 'EMEA'                 WHEN                             SUBSTR(CURRENCY_PAIR,4) IN                         (                         'ANG','ARS','AWG','BBD','BMD','BOB','BOV','BRL','BSD','BZD',                         'CLF','CLP','COP','CRC','CUP','DOP','ECU','FKP','GTQ','GYD','HNL',                         'HTG','JMD','KYD','MXN','MXV','NIO','PAB','PEN','PYG','SVC',                         'TTD','UYU','VEB'                         ) THEN 'LATAM'                 WHEN                     SUBSTR(CURRENCY_PAIR,4) IN                         (                         'AUD','CAD','CHF','EUR','GBP','JPY','NOK','NZD','SEK'                         ) THEN 'MAJORS'                 WHEN                     SUBSTR(CURRENCY_PAIR,4) IN                         (                         'DKK'                         ) THEN 'PEGGED'                 WHEN                     SUBSTR(CURRENCY_PAIR,4) IN                         (                         'USD','UBD'                         ) THEN 'USD'                 ELSE                     SUBSTR(CURRENCY_PAIR,4)             END         ) C     ) D GROUP BY     D.MAPPING,     D.CURRENCY_PAIR,     D.SPOT,     D.VOL