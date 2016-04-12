********************************
** this do-file compares employment
** and persons employed
********************************


log close _all
log using "$logDir/emp_vs_psn_emp.smcl", replace

local csvPath = "$tableDir/emp_vs_psn_emp.csv"
capture rm "`csvPath'"

use "processed_data/nonfin_OECD_eurostat_activity.dta", clear

ren iso3_d iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", ///
    keepusing(iso3 report_*_in) nogen // drop iso3_d not among reporting countries in Eurostat or OECD
ren iso3 iso3_d

ren iso3_o iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", ///
    keepusing(iso3 report_*_out) nogen
ren iso3 iso3_o

foreach x1 in es oecd {
    gen diff_`x1'_in_emp_2 = `x1'_in_n_emp_totXfin / `x1'_in_n_psn_emp_totXfin - 1
    foreach x2 in emp psn_emp {
        gen des_`x2' = "missing" if `x1'_in_n_`x2'_totXfin == .
        replace des_`x2' = "zero" if `x1'_in_n_`x2'_totXfin == 0
        replace des_`x2' = "positive" if `x1'_in_n_`x2'_totXfin > 0 & `x1'_in_n_`x2'_totXfin<.
    }
    estpost tabulate des_emp des_psn_emp, missing
    esttab using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle  ///
        title("Compare inward emp and psn_emp - source `x1'") eqlabels(, lhs("emp \ psn emp"))
    drop des_emp des_psn_emp
}

estpost sum diff_*_in_emp_2, detail
esttab . using "`csvPath'", append cells("mean sd count min p1 p10 p25 p50 p75 p90 p99 max") noobs ///
    title("Summary stats for difference of employment and person employed")
    
log close _all