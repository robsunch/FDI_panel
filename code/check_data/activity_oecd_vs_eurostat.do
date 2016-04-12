***************************************************
** compare OECD and Eurostat statistics on MNE activities
**
** coverage and differences in the common sample
**
***************************************************

log close _all
log using "$logDir/activity_oecd_vs_eurostat.smcl", replace

local csvPath = "$tableDir/activity_oecd_vs_eurostat.csv"
capture rm "`csvPath'"
local xlsxPath = "$tableDir/activity_oecd_vs_eurostat.xlsx"
capture rm "`xlsxPath'"

use "processed_data/nonfin_OECD_eurostat_activity.dta", clear

ren iso3_d iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", ///
    keepusing(iso3 report_*_in) nogen // drop iso3_d not among reporting countries in Eurostat or OECD
ren iso3 iso3_d

ren iso3_o iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", ///
    keepusing(iso3 report_*_out) nogen
ren iso3 iso3_o

*** coverage ***
/*
foreach x in in out {
    if "`x'"=="in" {
        local rev_x out
        local iso3_x d
    }
    else {
        local rev_x in
        local iso3_x o
    }
    
    preserve
    quietly ds *_`rev_x'_* *flag* iso3* year report_*_`x', not
    local varList `r(varlist)'
    collapse (count) `varList' (first) report_*_`x', by(iso3_`iso3_x' year)
    egen temp = rowtotal(`varList')
    drop if temp==0
    drop temp
    drop if report_es_`x' ~= 1 & report_oecd_`x' ~= 1
    sort iso3_`iso3_x' year
    order iso3_`iso3_x' year report* es* oecd*
    export excel using "`xlsxPath'", sheetreplace sheet("`x'_by_host_year") ///
        firstrow(variables)
    restore

    preserve
    quietly ds *_`rev_x'_* *flag* iso3* year report_*_`x', not
    local varList `r(varlist)'
    collapse (count) `varList' (first) report_*_`x', by(iso3_*)
    egen temp = rowtotal(`varList')
    drop if temp==0
    drop temp
    drop if report_es_`x' ~= 1 & report_oecd_`x' ~= 1
    sort iso3_d iso3_o
    order iso3_d iso3_o report* es* oecd*
    export excel using "`xlsxPath'", sheetreplace sheet("`x'_by_host_home") ///
        firstrow(variables)
    restore

}
*/

*** difference in varibales
foreach x in n_emp n_ent rev n_psn_emp {
    foreach s in es oecd {
        gen des_`s'_in_`x' = "missing" if `s'_in_`x'==.
        replace des_`s'_in_`x' = "=0" if `s'_in_`x'==0
        replace des_`s'_in_`x' = "<0" if `s'_in_`x'<0
        replace des_`s'_in_`x' = ">0" if `s'_in_`x'<. & `s'_in_`x'>0
    }
    estpost tabulate des_es_in_`x' des_oecd_in_`x' if report_es_in==1 & report_oecd_in==1, missing
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
        title("Different cases of inward `x'") eqlabels(, lhs("es\oecd"))
    
    gen diff_in_`x' = (es_in_`x'_totXfin - oecd_in_`x'_totXfin) ///
        / ((es_in_`x'_totXfin + oecd_in_`x'_totXfin)/2)
}

estpost summarize diff_in_* if report_es_in==1 & report_oecd_in==1, detail
esttab . using "`csvPath'", append cells("mean sd count min p1 p10 p25 p50 p75 p90 p99 max") noobs ///
    title("Summary stats for difference of inward variables between OECD and Eurostat (divide using average)")

drop diff_in_*

** preserve
foreach x in n_emp n_ent rev n_psn_emp {
    foreach s in oecd es {
        replace `s'_in_`x'_totXfin = . if `s'_in_`x'_totXfin <= 0
    }
    gen diff_in_`x' = ( es_in_`x'_totXfin - oecd_in_`x'_totXfin) ///
        / ((es_in_`x'_totXfin + oecd_in_`x'_totXfin)/2)
        
    estpost tabulate iso3_d if report_es_in==1 & report_oecd_in==1 & ///
        abs(diff_in_`x')>0.1 & diff_in_`x'<., sort
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
        title("Percentage Differences in inward `x' between OECD and ES > 0.5")
}
estpost summarize diff_in_* if report_es_in==1 & report_oecd_in==1, detail
esttab . using "`csvPath'", append cells("mean sd count min p1 p10 p25 p50 p75 p90 p99 max") noobs ///
    title("Summary stats for difference of inward variables between OECD and Eurostat (divide using average) - exclude nonpositive values of either source")

sort iso3_d iso3_o year
browse iso3_o year es_in_rev* oecd_in_rev* if iso3_d=="HUN"
** restore


    
log close _all
