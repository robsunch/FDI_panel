***************************************************
** this do-file compares data reported by host and home countries
**
***************************************************

log close _all
log using "$logDir/activity_inward_vs_outward.smcl", replace

local csvPath = "$tableDir/activity_inward_vs_outward.csv"
capture rm "`csvPath'"
local xlsxPath = "$tableDir/activity_inward_vs_outward.xlsx"
capture rm "`xlsxPath'"

use "processed_data/nonfin_OECD_eurostat_activity.dta", clear

** combine information in n_emp and n_psn_emp
foreach s in oecd es {
foreach direc in in out {
    gen `s'_`direc'_emp_totXfin = .
    capture confirm variable `s'_`direc'_n_psn_emp_totXfin
    if ~_rc {
        disp as text "Use Number of Persons employed as primary source for `s' `direc'."
        replace `s'_`direc'_emp_totXfin = `s'_`direc'_n_psn_emp_totXfin
        capture confirm variable `s'_`direc'_n_emp_totXfin_WRX
        if ~_rc {
            disp as text "Supplement with Number of employmees."
            replace `s'_`direc'_emp_totXfin = `s'_`direc'_n_emp_totXfin if `s'_`direc'_emp_totXfin <.
        }            
    }
    else {
        disp as text "Use Number of employees as primary source for `s' `direc'."
        replace `s'_`direc'_emp_totXfin = `s'_`direc'_n_emp_totXfin
    }    
}
}

*** compare coverage and differences between inward and outward statistics
foreach s in oecd es {
foreach x in emp rev {
    foreach direc in in out {
        gen des_`direc' = "miss" if missing(`s'_`direc'_`x'_totXfin)
        replace des_`direc' = ">0" if `s'_`direc'_`x'_totXfin>0 & `s'_`direc'_`x'_totXfin<.
        replace des_`direc' = "=0" if `s'_`direc'_`x'_totXfin==0
        replace des_`direc' = "<0" if `s'_`direc'_`x'_totXfin<0
    }
    estpost tabulate des_out des_in, quietly elabels
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
        title("Different cases of `s' `x'") eqlabels(, lhs("outward\inward"))  
    
    gen diff_`s'_`x' = `s'_out_`x'_totXfin / `s'_in_`x'_totXfin - 1 ///
        if des_in==">0" & des_out==">0"
    drop des_*
}
}

estpost summarize diff_*, detail quietly
esttab . using "`csvPath'", append cells("mean sd count min p1 p5 p10 p25 p50 p75 p90 p95 p99 max") noobs ///
    title("Summary stats for difference between stats reported by host and home countries")
drop diff_*

**** combine OECD and Eurostat information
foreach direc in in out {
    if "`direc'" == "in" {
        local suf = "d"
        local reverse_suf = "o"
        local reverse_direc = "out"
    }
    else {
        local suf = "o"
        local reverse_suf = "d"
        local reverse_direc = "in"
    }
    
    ** combine two sources
    ren iso3_`suf' iso3
    merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keepusing(report_*_`direc') nogen
    ren iso3 iso3_`suf'
    foreach x in rev emp {
        gen `direc'_`x' = es_`direc'_`x' if report_es_`direc'==1
        replace `direc'_`x' = oecd_`direc'_`x' if report_oecd_`direc'==1 & ///
            `direc'_`x'==.
    }
   
    ** world total from OECD or Eurostat
    preserve    
    keep if iso3_`reverse_suf'=="WRX"
    keep iso3_`suf' year `direc'_rev `direc'_emp
    ren (iso3_`suf' `direc'_rev `direc'_emp) ///
        (iso3 WRX_`direc'_rev WRX_`direc'_emp)
    tempfile `direc'_WRX
    save ``direc'_WRX', replace
    restore   

    ** aggregate over bilateral MNE sales for reporting countries
    preserve
    keep if report_oecd_`direc'==1 | report_es_`direc'==1
    drop if inlist(iso3_`reverse_suf',"WRX","WRT")
    foreach x in rev emp {
        gen num_nonmiss_`direc'_`x' = `direc'_`x' < .
    }
    ren iso3_`suf' iso3
    collapse (sum) `direc'_rev `direc'_emp num_nonmiss_*, by(iso3 year)
    replace `direc'_rev = . if num_nonmiss_`direc'_rev==0
    replace `direc'_emp = . if num_nonmiss_`direc'_emp==0
    tempfile `direc'_agg_report
    save ``direc'_agg_report', replace
    restore
    
    drop report_*_`direc'
    
    ** aggregate over bilateral MNE sales for nonreporting countries
    ** direction is defined in the reporting country's view
    preserve
    ren iso3_`reverse_suf' iso3
    merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keepusing(report_*_`reverse_direc') nogen
    keep if report_oecd_`reverse_direc'~=1 & ///
        report_es_`reverse_direc'~=1
    drop if inlist(iso3_`suf',"WRX","WRT") | inlist(iso3,"WRX","WRT")
    foreach x in rev emp {
        gen num_nonmiss_`direc'_`x' = `direc'_`x' < .
    }
    collapse (sum) `direc'_rev `direc'_emp num_nonmiss_*, by(iso3 year)
    replace `direc'_rev = . if num_nonmiss_`direc'_rev==0
    replace `direc'_emp = . if num_nonmiss_`direc'_emp==0
    
    tempfile `direc'_agg_nonreport
    save ``direc'_agg_nonreport', replace
    restore
    
}

** merge different datasets
use `in_agg_report', clear
merge 1:1 iso3 year using `out_agg_report', nogen
merge 1:1 iso3 year using `in_WRX', nogen
merge 1:1 iso3 year using `out_WRX', nogen

foreach direc in in out {
foreach x in rev emp {
    gen diff_`direc'_`x' = `direc'_`x' / WRX_`direc'_`x' - 1  
}
}

estpost summarize diff_*, quietly detail
esttab . using "`csvPath'", append cells("mean sd count min p1 p5 p10 p25 p50 p75 p90 p95 p99 max") noobs ///
    title("Differences between aggregate bilateral and WRX reported by source")
drop diff_*

use `in_agg_nonreport', clear
merge 1:1 iso3 year using `out_agg_nonreport', nogen
foreach direc in in out {
foreach x in emp rev {
    estpost tabulate num_nonmiss_`direc'_`x', sort elabels
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
        title("Number of nonmissing `direc' `x' from reporting countries' view")
}
}

*** number of reporting countries over time
set graphics off
encode iso3, gen(id_iso3)
xtset id_iso3 year
local largeCtyList CHN RUS BRA ZAF IND // "brics"
local figureFolder = "$figureDir/large_nonreporting_country_quality"
capture mkdir "`figureFolder'"
foreach cty in `largeCtyList' {
    twoway (tsline num_nonmiss_in_rev if iso3=="`cty'") ///
           (tsline num_nonmiss_out_rev if iso3=="`cty'", recast(connect)) ///
           , legend(order(1 "out rev" 2 "in rev")) title("`cty'")
    graph export "`figureFolder'/num_nonmiss_`cty'.pdf", replace
}

merge 1:1 iso3 year using "processed_data/agg_extrap.dta", keep(master match) nogen
foreach direc in in out {
    gen share_`direc'_emp = `direc'_emp / emp / 1e6
    gen share_`direc'_rev = `direc'_rev / nonfin_output_extrap
}

foreach cty in `largeCtyList' {
    twoway (tsline share_in_emp if iso3=="`cty'") ///
           (tsline share_in_rev if iso3=="`cty'", lpattern(dash_dot)) ///
           (tsline share_out_emp if iso3=="`cty'", yaxis(2) recast(connected)) ///
           (tsline share_out_rev if iso3=="`cty'", yaxis(2) lpattern(dash_dot) recast(connected)) ///
           , title("`cty' (share)") legend(order(1 "out emp" 2 "out rev" 3 "in emp" 4 "in rev"))
    graph export "`figureFolder'/agg_trend_`cty'.pdf", replace
}

set graphics on
log close _all
