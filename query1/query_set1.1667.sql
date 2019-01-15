select COB_DATE AS "Cob Date", ATTRIBUTE1 AS "Asset", ATTRIBUTE2 AS "Legal Entity", ATTRIBUTE3 AS "Business Unit", case when ATTRIBUTE4 
in ('CREDIT-CORPORATES', 'SECURITIZED PRODUCTS GRP','FXEM MACRO TRADING','EM CREDIT TRADING','LIQUID FLOW RATES','STRUCTURED RATES','DSP - CREDIT') then ATTRIBUTE4 when ATTRIBUTE3 in ('INSTITUTIONAL EQUITY DIVISION') then 'IED' else 'OTHER' end AS "Sub Business Unit", ATTRIBUTE5 AS "Desk", ATTRIBUTE6 AS "Book", ATTRIBUTE7 AS "Counterparty", ATTRIBUTE8 AS "Currency of Exposure", ATTRIBUTE9 AS "Scenario Shock", ATTRIBUTE10 AS "Severity", ATTRIBUTE11 AS "Country of Issuer", ATTRIBUTE12 AS "Product Type", ATTRIBUTE13 AS "Reference", ATTRIBUTE14 AS "FX Pair Currency", ATTRIBUTE15 AS "Source Table", ATTRIBUTE16 AS "Reporting Region", ATTRIBUTE17 AS "PH Level 7", ATTRIBUTE18 AS "Executive Model", ATTRIBUTE19 AS "Legal Entity Code", ATTRIBUTE20 AS "Curve Currency", ATTRIBUTE21 AS "Severity Spot", ATTRIBUTE22 AS "Severity Vol", ATTRIBUTE23 AS "Severity Rot", ATTRIBUTE24 AS "Severity Def", ATTRIBUTE25 AS "Severity Div", ATTRIBUTE26 AS "Severity Rec", ATTRIBUTE27 AS "Severity Corr", ATTRIBUTE28 AS "Credit Type", VALUE1 AS "Scenario Shock Result Value" from CDWUSER.U_GENERIC_DATA where cob_date in ('2018-02-16','2018-01-19','2017-12-29','2017-11-17','2017-10-20') and analytic_group = 'FDSF' and analytics = 'SCENARIOS_DETAILS' and ATTRIBUTE1 = 'CR'