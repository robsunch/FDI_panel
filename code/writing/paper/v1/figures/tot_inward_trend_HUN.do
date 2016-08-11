**********************************************
** this do-file checks total inward or outward FDI activities
** trends over time
**
**********************************************

log close _all
log using "$logDir/total_activity_trends.smcl", replace

local figurePath = "$figureDir/total_activity_trends"
capture mkdir "`figurePath'"
set graphics off

************************
*** total inward or outward ***
************************
foreach direc in in out {

    if "`direc'" == "in" {
        local x = "d"
        local rev_x = "o"
    }
    else {
        local x = "o"
        local rev_x = "d"
    }

    ** country aggregate using RRT data
    use "source_data/Ramondo-appendix-data/appendix-dataset.dta", clear
    merge 1:1 ISO_d ISO_o using "source_data/Ramondo-appendix-data/bilateral_mp.dta", keepusing(MandA) keep(master match) nogen
    foreach x2 in o d {
        replace ISO_`x2' = "DEU" if ISO_`x2'=="GER"
        replace ISO_`x2' = "ROU" if ISO_`x2'=="ROM"
        replace ISO_`x2' = "SVN" if ISO_`x2'=="SLV"
        ren ISO_`x2' iso3_`x2'
        }
    
    foreach x1 in MandA raw {    
        preserve
        egen num_posi = total(sales_`x1' > 0 & sales_`x1' < .), by(iso3_`x')
        egen num_miss = total(sales_`x1' == .), by(iso3_`x')
        collapse (sum) sales_`x1' (first) num_posi num_miss, by(iso3_`x')
        drop if num_posi == 0 & num_miss > 0
        keep iso3_`x' sales_`x1'
        tempfile `direc'_sales_`x1'_rrt
        save ``direc'_sales_`x1'_rrt', replace
        restore
    }

    use "processed_data/nonfin_OECD_eurostat_activity.dta", clear
    drop *flag* *inv_tangi* *n_ent* *prod_v* *psn_cost* *purchase* *vadd* *unpaid* // drop irrelevant variables
    ren iso3_`x' iso3
    merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", nogen
    keep if (report_oecd_`direc'==1 | report_es_`direc'==1)
       
    *** use world total reported by countries
    if "`direc'" == "in" {
        drop *out* report_*
        keep if inlist(iso3_`rev_x',"WRX","WRT")
        replace iso3_`rev_x' = "_" + iso3_`rev_x'
        ds iso3_o iso3 year, not
        reshape wide `r(varlist)', i(iso3 year) j(iso3_`rev_x') string
    }
    else {
        keep if inlist(iso3_`rev_x',"WRX")
        keep *out* iso3 iso3_d year
        ren *out* *out*_WRX
    }
    
    merge 1:1 iso3 year using "processed_data/agg_extrap.dta", nogen keep(master match)
    ren iso3 iso3_`x'
    merge m:1 iso3_`x' using ``direc'_sales_MandA_rrt', keep(master match) nogen
    merge m:1 iso3_`x' using ``direc'_sales_raw_rrt', keep(master match) nogen

    encode iso3_`x', gen(id_iso3_`x')
    xtset id_iso3_`x' year

    foreach s in oecd es {
        gen `s'_`direc'_rev_share = `s'_`direc'_rev_totXfin_WRX / nonfin_output_extrap
        if "`direc'" == "in" {
            gen `s'_`direc'_rev_share_fats = `s'_`direc'_rev_totXfin_WRX / `s'_`direc'_rev_totXfin_WRT
        }
        capture confirm variable `s'_`direc'_n_psn_emp_totXfin_WRX
        if ~_rc {
            disp as text "Use Number of Persons employed as primary source."
            gen `s'_`direc'_emp_share = `s'_`direc'_n_psn_emp_totXfin_WRX / emp / 1e6
            if "`direc'" == "in" {
                gen `s'_`direc'_emp_share_fats = `s'_`direc'_n_psn_emp_totXfin_WRX ///
                    / `s'_`direc'_n_psn_emp_totXfin_WRT
            }
            capture confirm variable `s'_`direc'_n_emp_totXfin_WRX
            if ~_rc {
                disp as text "Supplement with Number of employmees."
                replace `s'_`direc'_emp_share = `s'_`direc'_n_emp_totXfin_WRX / emp / 1e6 ///
                    if `s'_`direc'_emp_share == .
                if "`direc'" == "in" {
                    replace `s'_`direc'_emp_share_fats = `s'_`direc'_n_emp_totXfin_WRX ///
                        / `s'_`direc'_n_emp_totXfin_WRT if `s'_`direc'_emp_share_fats == .
                }                   
            }
        }
        else {
            disp as text "Use Number of employees as primary source."
            gen `s'_`direc'_emp_share = `s'_`direc'_n_emp_totXfin_WRX / emp / 1e6
            if "`direc'" == "in" {
                gen `s'_`direc'_emp_share_fats = `s'_`direc'_n_emp_totXfin_WRX ///
                    / `s'_`direc'_n_emp_totXfin_WRT
            }
        }
    }

    egen avg_nonfin_output = mean(nonfin_output_extrap) if year>=1996 & year<=2001, by(iso3_`x')
    gen sales_MandA_`direc'_output = sales_MandA/avg_nonfin_output
    gen sales_raw_`direc'_output = sales_raw/avg_nonfin_output

    ** plot time series by host countries
    quietly levelsof iso3_`x'
    foreach cty in `r(levels)' {
        macro drop _sales_MandA_`direc'_share _sales_raw_`direc'_share
        foreach x1 in MandA raw {
            quietly sum sales_`x1'_`direc'_output if iso3_`x'=="`cty'"
            local sales_`x1'_`direc'_share = r(mean)
        }
        if "`direc'" == "out" {
            if `sales_MandA_`direc'_share'==. & `sales_raw_`direc'_share'==. {
                twoway (tsline oecd_`direc'_rev_share if iso3_`x'=="`cty'", lpattern(dash_dot)) ///
                       (tsline es_`direc'_rev_share if iso3_`x'=="`cty'") ///
                       , title("`cty'") legend(order(1 "oecd" 2 "eurostat"))
            }
            else if `sales_MandA_`direc'_share'<. & `sales_raw_`direc'_share'==. {
                twoway (tsline oecd_`direc'_rev_share if iso3_`x'=="`cty'", lpattern(dash_dot)) ///
                       (tsline es_`direc'_rev_share if iso3_`x'=="`cty'") ///
                       , title("`cty'") legend(order(1 "oecd" 2 "eurostat")) ///
                       yline(`sales_MandA_`direc'_share') ///
                       note("horizontal line is sales (M&A) share from RRT")
            }
            else if `sales_MandA_`direc'_share'==. & `sales_raw_`direc'_share'<. {
                twoway (tsline oecd_`direc'_rev_share if iso3_`x'=="`cty'", lpattern(dash_dot)) ///
                       (tsline es_`direc'_rev_share if iso3_`x'=="`cty'") ///
                       , title("`cty'") legend(order(1 "oecd" 2 "eurostat")) ///
                       yline(`sales_raw_`direc'_share', lpattern(dash_dot)) ///
                       note("horizontal line is sales (M&A) share from RRT, dashed one is sales (raw)")
            }
            else {
                twoway (tsline oecd_`direc'_rev_share if iso3_`x'=="`cty'", lpattern(dash_dot)) ///
                       (tsline es_`direc'_rev_share if iso3_`x'=="`cty'") ///
                       , title("`cty'") legend(order(1 "oecd" 2 "eurostat")) ///
                       yline(`sales_MandA_`direc'_share') ///
                       yline(`sales_raw_`direc'_share', lpattern(dash_dot)) ///
                       note("horizontal line is sales (M&A) share from RRT, dashed one is sales (raw)")
            }    
            graph export "`figurePath'/`direc'_output_share_`cty'.pdf", replace
            
            twoway (tsline oecd_`direc'_emp_share if iso3_`x'=="`cty'" & oecd_`direc'_emp_share<2, lpattern(dash_dot) ) ///
                   (tsline es_`direc'_emp_share if iso3_`x'=="`cty'" & es_`direc'_emp_share<2 ) ///
                   , title("`cty'") legend(order(1 "oecd" 2 "eurostat"))
            graph export "`figurePath'/`direc'_emp_share_`cty'.pdf", replace   
        }
        else {
            if `sales_MandA_`direc'_share'==. & `sales_raw_`direc'_share'==. {
                twoway (tsline oecd_`direc'_rev_share if iso3_`x'=="`cty'", lpattern(dash_dot)) ///
                       (tsline oecd_`direc'_rev_share_fats if iso3_`x'=="`cty'", lpattern(dash_dot) recast(connected)) ///
                       (tsline es_`direc'_rev_share if iso3_`x'=="`cty'") ///
                       (tsline es_`direc'_rev_share_fats if iso3_`x'=="`cty'", recast(connected)) ///
                       , title("`cty'") legend(order(1 "oecd" 2 "oecd-fats-GO" 3 "es" 4 "es-fats-GO"))
            }
            else if `sales_MandA_`direc'_share'<. & `sales_raw_`direc'_share'==. {
                twoway (tsline oecd_`direc'_rev_share if iso3_`x'=="`cty'", lpattern(dash_dot)) ///
                       (tsline oecd_`direc'_rev_share_fats if iso3_`x'=="`cty'",lpattern(dash_dot) recast(connected)) ///
                       (tsline es_`direc'_rev_share if iso3_`x'=="`cty'") ///
                       (tsline es_`direc'_rev_share_fats if iso3_`x'=="`cty'", recast(connected)) ///
                       , title("`cty'") legend(order(1 "oecd" 2 "oecd-fats-GO" 3 "es" 4 "es-fats-GO")) ///
                       yline(`sales_MandA_`direc'_share') ///
                       note("horizontal line is sales (M&A) share from RRT")
            }
            else if `sales_MandA_`direc'_share'==. & `sales_raw_`direc'_share'<. {
                twoway (tsline oecd_`direc'_rev_share if iso3_`x'=="`cty'", lpattern(dash_dot)) ///
                       (tsline oecd_`direc'_rev_share_fats if iso3_`x'=="`cty'",lpattern(dash_dot) recast(connected)) ///
                       (tsline es_`direc'_rev_share if iso3_`x'=="`cty'") ///
                       (tsline es_`direc'_rev_share_fats if iso3_`x'=="`cty'", recast(connected)) ///
                       , title("`cty'") legend(order(1 "oecd" 2 "oecd-fats-GO" 3 "es" 4 "es-fats-GO")) ///
                       yline(`sales_raw_`direc'_share', lpattern(dash_dot)) ///
                       note("horizontal line is sales (M&A) share from RRT, dashed one is sales (raw)")
            }
            else {
                twoway (tsline oecd_`direc'_rev_share if iso3_`x'=="`cty'", lpattern(dash_dot)) ///
                       (tsline oecd_`direc'_rev_share_fats if iso3_`x'=="`cty'",lpattern(dash_dot) recast(connected)) ///
                       (tsline es_`direc'_rev_share if iso3_`x'=="`cty'") ///
                       (tsline es_`direc'_rev_share_fats if iso3_`x'=="`cty'", recast(connected)) ///
                       , title("`cty'") legend(order(1 "oecd" 2 "oecd-fats-GO" 3 "es" 4 "es-fats-GO")) ///
                       yline(`sales_MandA_`direc'_share') ///
                       yline(`sales_raw_`direc'_share', lpattern(dash_dot)) ///
                       note("horizontal line is sales (M&A) share from RRT, dashed one is sales (raw)")
            }    
            graph export "`figurePath'/`direc'_output_share_`cty'.pdf", replace
            
            twoway (tsline oecd_`direc'_emp_share if iso3_`x'=="`cty'" & oecd_`direc'_emp_share<2, lpattern(dash_dot) ) ///
                   (tsline oecd_`direc'_emp_share_fats if iso3_`x'=="`cty'" & oecd_`direc'_emp_share_fats<2, lpattern(dash_dot) recast(connected) ) ///
                   (tsline es_`direc'_emp_share if iso3_`x'=="`cty'" & es_`direc'_emp_share<2 ) ///
                   (tsline es_`direc'_emp_share_fats if iso3_`x'=="`cty'" & es_`direc'_emp_share_fats<2, recast(connected) ) ///
                   , title("`cty'") legend(order(1 "oecd" 2 "oecd-fats-GO" 3 "es" 4 "es-fats-GO"))
            graph export "`figurePath'/`direc'_emp_share_`cty'.pdf", replace           
        }
        
    }

}

set graphics on
