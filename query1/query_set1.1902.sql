WITH SHOCK AS ( with shock_format as ( SELECT shock_point,avg(hypercube_shift) as hypercube_shift, case when scenario_name like '%001%' then 'BHC2017_S1' when scenario_name like '%011%' then 'FRB2017_SA'  when scenario_name like '%021%' then 'BAU2017_S1'  end AS scn, case  when shock_point like 'STSCD=3M:0M' then 'usdn_3m0M' when shock_point like 'STSCD=3M:1M' then 'usdn_3m1M' when shock_point like 'IRS=4Y' then 'usdn_3m4Y' when shock_point like 'IRS=5Y' then 'usdn_3m5Y' when shock_point like 'IRS=6Y' then 'usdn_3m6Y' when shock_point like 'IRS=7Y' then 'usdn_3m7Y' when shock_point like 'IRS=8Y' then 'usdn_3m8Y' when shock_point like 'IRS=9Y' then 'usdn_3m9Y' when shock_point like 'IRS=10Y' then 'usdn_3m10Y' when shock_point like 'IRS=12Y' then 'usdn_3m12Y' when shock_point like 'IRS=15Y' then 'usdn_3m15Y' when shock_point like 'IRS=20Y' then 'usdn_3m20Y' when shock_point like 'IRS=25Y' then 'usdn_3m25Y' when shock_point like 'IRS=30Y' then 'usdn_3m30Y' when shock_point like 'IRS=35Y' then 'usdn_3m35Y' when shock_point like 'IRS=40Y' then 'usdn_3m40Y' when shock_point like 'IRS=50Y' then 'usdn_3m50Y' when shock_point like 'IRS=60Y' then 'usdn_3m60Y' when shock_point like 'IRS=75Y' then 'usdn_3m75Y' else 'unstatic' end as shock_point_static FROM dwuser.u_modular_scenario_shock  where COB_DATE = '2018-02-28' AND process_id = 62049 and curve_name like '%usdn_3m%' and (scenario_name like '%001%' or scenario_name like '%011%' or scenario_name like '%021%') AND slice_name LIKE '%NONPRA%' GROUP BY shock_point,scenario_name  ) select *, case  when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '01' then 'usdn_3m'||substring(shock_point,16)||'-Jan-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '02' then 'usdn_3m'||substring(shock_point,16)||'-Feb-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '03' then 'usdn_3m'||substring(shock_point,16)||'-Mar-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '04' then 'usdn_3m'||substring(shock_point,16)||'-Apr-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '05' then 'usdn_3m'||substring(shock_point,16)||'-May-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '06' then 'usdn_3m'||substring(shock_point,16)||'-Jun-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '07' then 'usdn_3m'||substring(shock_point,16)||'-Jul-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '08' then 'usdn_3m'||substring(shock_point,16)||'-Aug-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '09' then 'usdn_3m'||substring(shock_point,16)||'-Sep-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '10' then 'usdn_3m'||substring(shock_point,16)||'-Oct-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '11' then 'usdn_3m'||substring(shock_point,16)||'-Nov-'||substring(shock_point,12,2) when shock_point_static = 'unstatic' and shock_point like 'STSCD=3M%' and substring(shock_point,14,2) = '12' then 'usdn_3m'||substring(shock_point,16)||'-Dec-'||substring(shock_point,12,2)  when shock_point_static = 'unstatic' and shock_point like 'IRF=EDH%' and cast(substring(shock_point,8) as int) < 10 and cast(substring(shock_point,8) as int) > 0 then 'usdn_3mMar1'||substring(shock_point,8)||'x3M' when shock_point_static = 'unstatic' and shock_point like 'IRF=EDH%' and cast(substring(shock_point,8) as int) >= 10 then 'usdn_3mMar'||substring(shock_point,8)||'x3M'  when shock_point_static = 'unstatic' and shock_point like 'IRF=EDM%' and cast(substring(shock_point,8) as int) < 10 and cast(substring(shock_point,8) as int) > 0 then 'usdn_3mJun1'||substring(shock_point,8)||'x3M' when shock_point_static = 'unstatic' and shock_point like 'IRF=EDM%' and cast(substring(shock_point,8) as int) >= 10 then 'usdn_3mJun'||substring(shock_point,8)||'x3M'  when shock_point_static = 'unstatic' and shock_point like 'IRF=EDU%' and cast(substring(shock_point,8) as int) < 10 and cast(substring(shock_point,8) as int) > 0 then 'usdn_3mSep1'||substring(shock_point,8)||'x3M' when shock_point_static = 'unstatic' and shock_point like 'IRF=EDU%' and cast(substring(shock_point,8) as int) >= 10 then 'usdn_3mSep'||substring(shock_point,8)||'x3M'  when shock_point_static = 'unstatic' and shock_point like 'IRF=EDZ%' and cast(substring(shock_point,8) as int) < 10 and cast(substring(shock_point,8) as int) > 0 then 'usdn_3mDec1'||substring(shock_point,8)||'x3M' when shock_point_static = 'unstatic' and shock_point like 'IRF=EDZ%' and cast(substring(shock_point,8) as int) >= 10 then 'usdn_3mDec'||substring(shock_point,8)||'x3M'    else shock_point_static end as shock_point_final from shock_format ), PV01 AS ( SELECT CURVE_REFERENCE_ENTITY_ID,SUM(usd_ir_unified_pv01) as PV01, CASE WHEN CCC_PRODUCT_LINE = 'US SWAPS' THEN 'US SWAPS' WHEN CCC_STRATEGY = 'US OPTIONS/EXOTICS' THEN 'US OPTIONS/EXOTICS' END AS REPORTING_LINE from DWUSER.U_IR_MSR   where COB_DATE = '2018-02-28' AND  (CCC_PRODUCT_LINE = 'US SWAPS' OR CCC_STRATEGY = 'US OPTIONS/EXOTICS') and CURVE_NAME='usdn_3m' and usd_ir_unified_pv01<>0 group by CURVE_REFERENCE_ENTITY_ID,CCC_STRATEGY,CCC_PRODUCT_LINE )  , TABLE_JOINT AS ( SELECT * FROM SHOCK INNER JOIN PV01 ON SHOCK.shock_point_final = PV01.CURVE_REFERENCE_ENTITY_ID )  SELECT *, cast(hypercube_shift as float) * cast(PV01 as float) AS pnl FROM TABLE_JOINT