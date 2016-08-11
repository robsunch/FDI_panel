************************************
** compare coverage of emp and psn_emp
************************************

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
    esttab . using "$tableDir/emp_psn_emp_cover_oecd_in_`suffix'.tex", replace cell(b) unstack ///
        noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
        title("Number of observations with nonmissing values OECD inward employment `suffix'") booktabs
       
    drop cover
    
    gen diff_log_oecd_in_`suffix' = log(oecd_in_n_emp_`suffix') - ///
        log(oecd_in_n_psn_emp_`suffix')
    
}

estpost summarize diff_log_*, quietly detail
local prehead_str "\begin{table}[h]\scriptsize\caption{Diff between psn emp and emp - oecd inward}\centering" ///
    "\begin{threeparttable}\begin{tabular}{l*{10}c}\toprule"
local postfoot_str "\bottomrule\end{tabular}\begin{tablenotes}" ///
    "\item[a] Diff in log points (emp - psn emp)." ///
    "\end{tablenotes}\end{threeparttable}\end{table}"
esttab . using "$tableDir/emp_psn_emp_diff_oecd_in.tex", replace ///
    noobs nonumber nomtitle nodepvars nostar booktabs ///
    cell("count(fmt(a3)) mean sd min p10 p25 p50 p75 p90 max") ///
    prehead("`prehead_str'") postfoot("`postfoot_str'")
    
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
    esttab using "$tableDir/emp_psn_emp_cover_oecd_out_`suffix'.tex", replace cell(b) unstack ///
        noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
        title("Number of observations with nonmissing values OECD outward employment `suffix'") booktabs
    drop cover

    gen diff_log_oecd_out_`suffix' = log(oecd_out_n_emp_`suffix') - ///
        log(oecd_out_n_psn_emp_`suffix')
    
}

estpost summarize diff_log_*, quietly detail
local prehead_str "\begin{table}[h]\scriptsize\caption{Diff between psn emp and emp - oecd outward}\centering" ///
    "\begin{threeparttable}\begin{tabular}{l*{10}c}\toprule"
local postfoot_str "\bottomrule\end{tabular}\begin{tablenotes}" ///
    "\item[a] Diff in log points (emp - psn emp)." ///
    "\end{tablenotes}\end{threeparttable}\end{table}"
esttab . using "$tableDir/emp_psn_emp_diff_oecd_out.tex", replace ///
    noobs nonumber nomtitle nodepvars nostar booktabs ///
    cell("count(fmt(a3)) mean sd min p10 p25 p50 p75 p90 max") ///
    prehead("`prehead_str'") postfoot("`postfoot_str'")
    
    
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
    esttab using "$tableDir/emp_psn_emp_cover_es_in_`suffix'.tex", replace cell(b) unstack ///
        noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
        title("Number of observations with nonmissing values Eurostat inward employment `suffix'") booktabs
    drop cover

    gen diff_log_es_in_`suffix' = log(es_in_n_emp_`suffix') - ///
        log(es_in_n_psn_emp_`suffix')
    
}

estpost summarize diff_log_*, quietly detail
local prehead_str "\begin{table}[h]\scriptsize\caption{Diff between psn emp and emp - eurostat inward}\centering" ///
    "\begin{threeparttable}\begin{tabular}{l*{10}c}\toprule"
local postfoot_str "\bottomrule\end{tabular}\begin{tablenotes}" ///
    "\item[a] Diff in log points (emp - psn emp)." ///
    "\end{tablenotes}\end{threeparttable}\end{table}"
esttab . using "$tableDir/emp_psn_emp_diff_es_in.tex", replace ///
    noobs nonumber nomtitle nodepvars nostar booktabs ///
    cell("count(fmt(a3)) mean sd min p10 p25 p50 p75 p90 max") ///
    prehead("`prehead_str'") postfoot("`postfoot_str'")    
    