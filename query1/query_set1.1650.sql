select COB_DATE AS "Cob Date", case when ATTRIBUTE3 in ('INSTITUTIONAL EQUITY DIVISION') then 'IED' when ATTRIBUTE4 in ('FXEM MACRO TRADING','LIQUID FLOW RATES','STRUCTURED RATES') then ATTRIBUTE4 else 'OTHER' end AS "Sub Business Unit", ATTRIBUTE21 AS "Severity Spot", ATTRIBUTE22 AS "Severity Vol", VALUE1 AS "Scenario Shock Result Value" from CDWUSER.U_GENERIC_DATA where cob_date in ('2018-02-16','2018-01-19') and analytic_group = 'FDSF' and analytics = 'SCENARIOS_DETAILS' and ATTRIBUTE1 = 'FX' and ATTRIBUTE10 not like '%w.%'