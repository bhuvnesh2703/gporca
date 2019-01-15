SELECT a.COB_DATE, a.ATTRIBUTE9, a.ATTRIBUTE10, CASE WHEN a.ATTRIBUTE17 in ('1M','2M','3M','6M','9M') THEN '<1 year' WHEN a.ATTRIBUTE17 in ('12M','2Y','5Y') THEN '1-5 years' WHEN a.ATTRIBUTE17 in ('10Y','20Y','30Y') THEN '>5 years' WHEN a.ATTRIBUTE17 in ('All') THEN 'All' END AS TERM, CASE WHEN a.ATTRIBUTE12 in ('Gold') THEN 'Gold' WHEN a.ATTRIBUTE12 in ('Silver') THEN 'Silver' WHEN a.ATTRIBUTE12 in ('Nickel') THEN 'Nickel' WHEN a.ATTRIBUTE12 in ('Zinc') THEN 'Zinc' WHEN a.ATTRIBUTE12 in ('Copper') THEN 'Copper' WHEN a.ATTRIBUTE12 in ('Crude Oil') THEN 'Crude Oil' ELSE 'Others' END AS UNDERLIER, sum(coalesce(a.VALUE1,0)) as VALUE1 FROM CDWUSER.U_GENERIC_DATA a WHERE a.ANALYTIC_GROUP = 'FDSF' AND a.COB_DATE IN ('2018-02-16') AND a.ATTRIBUTE1 = 'CM' AND ATTRIBUTE9 in ('CM Delta','CM Skew','CM Smile','CM Vanna','CM Vega') AND ANALYTICS = 'SENSITIVITIES' GROUP BY a.COB_DATE, a.ATTRIBUTE9, a.ATTRIBUTE10, CASE WHEN a.ATTRIBUTE17 in ('1M','2M','3M','6M','9M') THEN '<1 year' WHEN a.ATTRIBUTE17 in ('12M','2Y','5Y') THEN '1-5 years' WHEN a.ATTRIBUTE17 in ('10Y','20Y','30Y') THEN '>5 years' WHEN a.ATTRIBUTE17 in ('All') THEN 'All' END, CASE WHEN a.ATTRIBUTE12 in ('Gold') THEN 'Gold' WHEN a.ATTRIBUTE12 in ('Silver') THEN 'Silver' WHEN a.ATTRIBUTE12 in ('Nickel') THEN 'Nickel' WHEN a.ATTRIBUTE12 in ('Zinc') THEN 'Zinc' WHEN a.ATTRIBUTE12 in ('Copper') THEN 'Copper' WHEN a.ATTRIBUTE12 in ('Crude Oil') THEN 'Crude Oil' ELSE 'Others' END