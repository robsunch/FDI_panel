***************************************
** this do-file consolidates employment variables
** use psn_emp as primary, supplement with emp
***************************************

log close _all
log using "$logDir/consolidate_emp.smcl", replace

local outputPath = "$tableDir/consolidate_emp.csv"
capture rm "`outputPath'"

*** check coverage and difference between the two variables 
*** for OECD inward/outward and Eurostat inward

** OECD inward
use "processed_data/activity_OECD_eurostat_combined.dta", replace
ren iso3_d iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(report_oecd_in) nogen
ren iso3 iso3_d
keep if report_oecd_in
keep oecd_in* iso3* year    

foreach suffix in tot totXfin {
    gen cover = 1 if oecd_in_n_emp_`suffix' <. & oecd_in_n_psn_emp_`suffix' ==.
    replace cover = 2 if oecd_in_n_emp_`suffix' ==. & oecd_in_n_psn_emp_`suffix' <.
    replace cover = 3 if oecd_in_n_emp_`suffix' <. & oecd_in_n_psn_emp_`suffix' <.
    capture label drop cover_lab
    label define cover_lab 1 "EMP only" 2 "PSN EMP only" 3 "Both"    
    label values cover cover_lab
    estpost tabulate year cover
    esttab using `outputPath', append cell(b) unstack ///
        noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
        title("Number of observations with nonmissing values OECD inward employment `suffix'")
       
    drop cover
    
    gen diff_log_oecd_in_`suffix' = log(oecd_in_n_emp_`suffix') - ///
        log(oecd_in_n_psn_emp_`suffix')
    
}
estpost summarize diff_log_*, quietly detail
esttab . using `outputPath', append ///
    noobs nonumber nomtitle nodepvars nostar ///
    addnote("Diff in log points (emp - psn emp)") ///
    title("Diff between psn emp and emp - oecd inward") ///
    cell("count(fmt(a3)) mean sd min p1 p5 p10 p25 p50 p75 p90 p95 p99 max")
    
*** OECD outward
use "processed_data/activity_OECD_eurostat_combined.dta", replace
ren iso3_o iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(report_oecd_out) nogen
ren iso3 iso3_o
keep if report_oecd_out
keep oecd_out* iso3* year   

foreach suffix in tot fin {
    gen cover = 1 if oecd_out_n_emp_`suffix' <. & oecd_out_n_psn_emp_`suffix' ==.
    replace cover = 2 if oecd_out_n_emp_`suffix' ==. & oecd_out_n_psn_emp_`suffix' <.
    replace cover = 3 if oecd_out_n_emp_`suffix' <. & oecd_out_n_psn_emp_`suffix' <.
    capture label drop cover_lab
    label define cover_lab 1 "EMP only" 2 "PSN EMP only" 3 "Both"        
    label values cover cover_lab
    estpost tabulate year cover
    esttab using `outputPath', append cell(b) unstack ///
        noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
        title("Number of observations with nonmissing values OECD outward employment `suffix'")
    drop cover

    gen diff_log_oecd_out_`suffix' = log(oecd_out_n_emp_`suffix') - ///
        log(oecd_out_n_psn_emp_`suffix')
    
}

estpost summarize diff_log_*, quietly detail
esttab . using `outputPath', append ///
    noobs nonumber nomtitle nodepvars nostar ///
    addnote("Diff in log points (emp - psn emp)") ///
    title("Diff between psn emp and emp - oecd outward") ///
    cell("count(fmt(a3)) mean sd min p1 p5 p10 p25 p50 p75 p90 p95 p99 max")
    
    
*** Eurostat inward
use "processed_data/activity_OECD_eurostat_combined.dta", replace
ren iso3_d iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(report_es_in) nogen
ren iso3 iso3_d
keep if report_es_in
keep es_in* iso3* year   

foreach suffix in totXfin {
    gen cover = 1 if es_in_n_emp_`suffix' <. & es_in_n_psn_emp_`suffix' ==.
    replace cover = 2 if es_in_n_emp_`suffix' ==. & es_in_n_psn_emp_`suffix' <.
    replace cover = 3 if es_in_n_emp_`suffix' <. & es_in_n_psn_emp_`suffix' <.
    capture label drop cover_lab
    label define cover_lab 1 "EMP only" 2 "PSN EMP only" 3 "Both"        
    label values cover cover_lab
    estpost tabulate year cover
    esttab using `outputPath', append cell(b) unstack ///
        noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
        title("Number of observations with nonmissing values Eurostat inward employment `suffix'")
    drop cover

    gen diff_log_es_in_`suffix' = log(es_in_n_emp_`suffix') - ///
        log(es_in_n_psn_emp_`suffix')
    
}

estpost summarize diff_log_*, quietly detail
esttab . using `outputPath', append ///
    noobs nonumber nomtitle nodepvars nostar ///
    addnote("Diff in log points (emp - psn emp)") ///
    title("Diff between psn emp and emp - eurostat inward") ///
    cell("count(fmt(a3)) mean sd min p1 p5 p10 p25 p50 p75 p90 p95 p99 max")     

    
*****************************************
** consolidate two variables
*****************************************
use "processed_data/activity_OECD_eurostat_combined.dta", replace

** OECD inward
foreach x in tot totXfin {
    replace oecd_in_flag_n_psn_emp_`x' = oecd_in_flag_n_emp_`x' if oecd_in_n_psn_emp_`x'==.
    replace oecd_in_n_psn_emp_`x' = oecd_in_n_emp_`x' if oecd_in_n_psn_emp_`x'==.
    drop oecd_in_n_emp_`x' oecd_in_flag_n_emp_`x'
    ren oecd_in_n_psn_emp_`x' oecd_in_n_emp_`x'
    ren oecd_in_flag_n_psn_emp_`x' oecd_in_flag_n_emp_`x'
}  

** OECD outward
foreach x in tot fin {
    replace oecd_out_flag_n_psn_emp_`x' = oecd_out_flag_n_emp_`x' if oecd_out_n_psn_emp_`x'==.
    replace oecd_out_n_psn_emp_`x' = oecd_out_n_emp_`x' if oecd_out_n_psn_emp_`x'==.
    drop oecd_out_n_emp_`x' oecd_out_flag_n_emp_`x'
    ren oecd_out_n_psn_emp_`x' oecd_out_n_emp_`x'
    ren oecd_out_flag_n_psn_emp_`x' oecd_out_flag_n_emp_`x'
}  

** Eurostat inward
foreach x in totXfin {
    replace es_in_flag_n_psn_emp_`x' = es_in_flag_n_emp_`x' if es_in_n_psn_emp_`x'==.
    replace es_in_n_psn_emp_`x' = es_in_n_emp_`x' if es_in_n_psn_emp_`x'==.
    drop es_in_n_emp_`x' es_in_flag_n_emp_`x'
    ren es_in_n_psn_emp_`x' es_in_n_emp_`x'
    ren es_in_flag_n_psn_emp_`x' es_in_flag_n_emp_`x'
}  

** Eurostat outward
ren es_out*n_psn_emp* es_out*n_emp*

compress
save "processed_data/activity_OECD_eurostat_consol_emp.dta", replace

log close _all
