********************************************************
** this do file exclude financial activities from 
** total business for OECD inward (year <= 2007)
** and outward (all years) and for Eurostat (all years)
********************************************************

log close _all
log using "$logDir/exclude_fin.smcl", replace

local outputPath = "$tableDir/exclude_fin.csv"
capture rm "`outputPath'"

** prepare for country level nonfinancial output share
use "processed_data/nonfin_output_share.dta", clear
fillin iso3 year
encode iso3, gen(id_iso3)
egen last_yr = max(year*(nonfin_output_share < .)), by(iso3)
gen temp = nonfin_output_share if year == last_yr
egen last_yr_share = max(temp), by(iso3)
replace nonfin_output_share = last_yr_share if year>last_yr
keep iso3 year nonfin_output_share
tempfile nonfin_output_share
save `nonfin_output_share', replace

***********************
** OECD inward
***********************

use "processed_data/activity_OECD_eurostat_combined.dta", replace
ren iso3_d iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(report_oecd_in) nogen
ren iso3 iso3_d
keep if report_oecd_in
keep oecd_in* iso3* year    

** approximate missing number of employees with persons employed
foreach x in tot totXfin {
    gen diff_emp_`x' = oecd_in_n_emp_`x'/oecd_in_n_psn_emp_`x' - 1
    replace oecd_in_n_emp_`x' = oecd_in_n_psn_emp_`x' if oecd_in_n_emp_`x'==.
}  
estpost summarize diff_emp_*, detail quietly
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("% diff number of employees v.s. number of persons employed OECD inward all years")
drop diff_emp* *psn_emp*

preserve
keep if iso3_o == "WRX"
fillin iso3_o iso3_d year
encode iso3_d, gen(id_iso3_d)
xtset id_iso3_d year
foreach x in n_emp n_ent rev {
    gen nonfin_by_tot_`x' = oecd_in_`x'_totXfin / oecd_in_`x'_tot
    replace nonfin_by_tot_`x' = 1 - oecd_in_`x'_fin / oecd_in_`x'_tot if nonfin_by_tot_`x'==.
    gen temp1 = l.nonfin_by_tot_`x'
    gen temp2 = f.nonfin_by_tot_`x'
    egen WRX_in_nonfin_share_`x' = rowmean(temp1 nonfin_by_tot_`x' temp2) // ignore missing values
    
    *** average nonfin_share of the last three years available
    egen nonmiss = rownonmiss(nonfin_by_tot_`x')
    egen last_yr = max(year*(nonmiss>0)), by(iso3_d)
    gen temp = WRX_in_nonfin_share_`x' if year == last_yr - 1 // last three year mean
    egen last_3yr_mean = max(temp), by(iso3_d)
    replace WRX_in_nonfin_share_`x' = last_3yr_mean if WRX_in_nonfin_share_`x' == . ///
        & year > last_yr
    drop temp* last_3yr_mean last_yr nonmiss
}

keep iso3_d year WRX_in_nonfin_share* 
tempfile WRX_in_nonfin_share
save `WRX_in_nonfin_share', replace
restore

** step 0 : simple subtraction
foreach x in n_ent n_emp rev {
    replace oecd_in_`x'_totXfin = oecd_in_`x'_tot - oecd_in_`x'_fin ///
        if oecd_in_`x'_totXfin == .
}
local countVarList "oecd_in_n_ent_totXfin oecd_in_n_emp_totXfin oecd_in_rev_totXfin"
egen nonmiss0 = rownonmiss(`countVarList')
gen exclude_fin_adj = "none" if nonmiss0 > 0
estpost tabulate nonmiss0, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_emp and rev in OECD inward activities after step 0: simple subtraction") varlabels(`e(labels)')
** step 1: use nonfinancial share of total inward MP
merge m:1 iso3_d year using `WRX_in_nonfin_share', keep(master match) nogen
foreach x in n_ent n_emp rev {
    replace oecd_in_`x'_totXfin = oecd_in_`x'_tot * WRX_in_nonfin_share_`x' ///
        if nonmiss0 == 0 & iso3_d~="WRT" // national total better use total nonfinancial output share
}
egen nonmiss1 = rownonmiss(`countVarList')
estpost tabulate nonmiss1, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_emp and rev in OECD inward activities after step 1: use nonfinancial share of total inward MP") varlabels(`e(labels)')
replace exclude_fin_adj = "total inward nonfinancial share" if nonmiss1 > nonmiss0
drop WRX_in_nonfin_share*
** step 2: use host country's share of nonfinancial output
ren iso3_d iso3
merge m:1 iso3 year using `nonfin_output_share', keep(master match) nogen
ren iso3 iso3_d
foreach x in n_ent n_emp rev {
    replace oecd_in_`x'_totXfin = oecd_in_`x'_tot * nonfin_output_share ///
        if nonmiss1 == 0
}        
egen nonmiss2 = rownonmiss(`countVarList')
estpost tabulate nonmiss2, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_emp and rev in OECD inward activities after step 2: use nonfinancial output share of the host country") varlabels(`e(labels)')
replace exclude_fin_adj = "host country nonfinancial output share" if nonmiss2 > nonmiss1
drop nonfin_output_share
** step 3: use home country's share of nonfinancial output
ren iso3_o iso3
merge m:1 iso3 year using `nonfin_output_share', keep(master match) nogen
ren iso3 iso3_o
foreach x in n_ent n_emp rev {
    replace oecd_in_`x'_totXfin = oecd_in_`x'_tot * nonfin_output_share ///
        if nonmiss2 == 0
}        
egen nonmiss3 = rownonmiss(`countVarList')
estpost tabulate nonmiss3, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_emp and rev in OECD inward activities after step 3: use nonfinancial output share of the home country") varlabels(`e(labels)')
replace exclude_fin_adj = "home country nonfinancial output share" if nonmiss3 > nonmiss2
drop nonfin_output_share

estpost tabulate exclude_fin_adj year, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Cases excluding financial sector in different steps (OECD inward)") varlabels(`e(labels)')
keep iso3* year oecd_in_*_totXfin
tempfile oecd_in
save `oecd_in', replace

**********************
** OECD outward
**********************
use "processed_data/activity_OECD_eurostat_combined.dta", replace
ren iso3_o iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(report_oecd_out) nogen
ren iso3 iso3_o
keep if report_oecd_out
keep oecd_out* iso3* year    

** approximate missing number of employees with persons employed
foreach x in tot fin {
    gen diff_emp_`x' = oecd_out_n_emp_`x'/oecd_out_n_psn_emp_`x' - 1
    replace oecd_out_n_emp_`x' = oecd_out_n_psn_emp_`x' if oecd_out_n_emp_`x'==.
}
capture estpost summarize diff_emp_*, detail quietly
capture esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("% diff number of employees v.s. number of persons employed OECD outward all years")
drop diff_emp* *psn_emp*

preserve
keep if iso3_d == "WRX"
fillin iso3_o iso3_d year
encode iso3_o, gen(id_iso3_o)
xtset id_iso3_o year
foreach x in n_emp n_ent rev {
    gen nonfin_by_tot_`x' = 1 - oecd_out_`x'_fin / oecd_out_`x'_tot
    gen temp1 = l.nonfin_by_tot_`x'
    gen temp2 = f.nonfin_by_tot_`x'
    egen WRX_out_nonfin_share_`x' = rowmean(temp1 nonfin_by_tot_`x' temp2) // ignore missing values
    drop temp1 temp2
    
    *** average nonfin_share of the last three years available
    egen nonmiss = rownonmiss(nonfin_by_tot_`x')
    egen last_yr = max(year*(nonmiss>0)), by(iso3_d)
    gen temp = WRX_out_nonfin_share_`x' if year == last_yr - 1 // last three year mean
    egen last_3yr_mean = max(temp), by(iso3_d)
    replace WRX_out_nonfin_share_`x' = last_3yr_mean if WRX_out_nonfin_share_`x' == . ///
        & year > last_yr
    drop temp* last_3yr_mean last_yr nonmiss    
}
keep iso3_o year WRX_out_nonfin_share*
tempfile WRX_out_nonfin_share
save `WRX_out_nonfin_share', replace
restore

** step 0 : simple subtraction
foreach x in n_ent n_emp rev {
    gen oecd_out_`x'_totXfin = oecd_out_`x'_tot - oecd_out_`x'_fin
}
local countVarList "oecd_out_n_ent_totXfin oecd_out_n_emp_totXfin oecd_out_rev_totXfin"
egen nonmiss0 = rownonmiss(`countVarList')
gen exclude_fin_adj = "none" if nonmiss0 > 0
estpost tabulate nonmiss0, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_emp and rev in OECD outward activities after step 0: simple subtraction") varlabels(`e(labels)')
** step 1: use nonfinancial share of total outward MP
merge m:1 iso3_o year using `WRX_out_nonfin_share', keep(master match) nogen
foreach x in n_ent n_emp rev {
    replace oecd_out_`x'_totXfin = oecd_out_`x'_tot * WRX_out_nonfin_share_`x' ///
        if nonmiss0 == 0
}
egen nonmiss1 = rownonmiss(`countVarList')
estpost tabulate nonmiss1, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_emp and rev in OECD outward activities after step 1: use nonfinancial share of total outward MP") varlabels(`e(labels)')
replace exclude_fin_adj = "total outward nonfinancial share" if nonmiss1 > nonmiss0
drop WRX_out_nonfin_share*
** step 2: use host country's share of nonfinancial output
ren iso3_d iso3
merge m:1 iso3 year using `nonfin_output_share', keep(master match) nogen
ren iso3 iso3_d
foreach x in n_ent n_emp rev {
    replace oecd_out_`x'_totXfin = oecd_out_`x'_tot * nonfin_output_share ///
        if nonmiss1 == 0
}        
egen nonmiss2 = rownonmiss(`countVarList')
estpost tabulate nonmiss2, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_emp and rev in OECD outward activities after step 2: use nonfinancial output share of the host country") varlabels(`e(labels)')
replace exclude_fin_adj = "host country nonfinancial output share" if nonmiss2 > nonmiss1
drop nonfin_output_share
** step 3: use home country's share of nonfinancial output
ren iso3_o iso3
merge m:1 iso3 year using `nonfin_output_share', keep(master match) nogen
ren iso3 iso3_o
foreach x in n_ent n_emp rev {
    replace oecd_out_`x'_totXfin = oecd_out_`x'_tot * nonfin_output_share ///
        if nonmiss2 == 0
}        
egen nonmiss3 = rownonmiss(`countVarList')
estpost tabulate nonmiss3, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_emp and rev in OECD outward activities after step 3: use nonfinancial output share of the home country") varlabels(`e(labels)')
replace exclude_fin_adj = "home country nonfinancial output share" if nonmiss3 > nonmiss2
drop nonfin_output_share

estpost tabulate exclude_fin_adj year, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Cases excluding financial sector in different steps (OECD outward)") varlabels(`e(labels)')
keep iso3* year oecd_out_*_totXfin
tempfile oecd_out
save `oecd_out', replace

*************************
** Eurostat outward
*************************
use "processed_data/activity_OECD_eurostat_combined.dta", replace
ren iso3_o iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(report_es_out) nogen
ren iso3 iso3_o
keep if report_es_out
keep es_out* iso3* year    

preserve
keep if iso3_d == "WRX"
fillin iso3_o iso3_d year
encode iso3_o, gen(id_iso3_o)
xtset id_iso3_o year
foreach x in n_psn_emp n_ent rev {
    gen nonfin_by_tot_`x' = 1 - es_out_`x'_fin / es_out_`x'_tot
    gen temp1 = l.nonfin_by_tot_`x'
    gen temp2 = f.nonfin_by_tot_`x'
    egen WRX_out_nonfin_share_`x' = rowmean(temp1 nonfin_by_tot_`x' temp2) // ignore missing values
    drop temp1 temp2
    
    *** average nonfin_share of the last three years available
    egen nonmiss = rownonmiss(nonfin_by_tot_`x')
    egen last_yr = max(year*(nonmiss>0)), by(iso3_d)
    gen temp = WRX_out_nonfin_share_`x' if year == last_yr - 1 // last three year mean
    egen last_3yr_mean = max(temp), by(iso3_d)
    replace WRX_out_nonfin_share_`x' = last_3yr_mean if WRX_out_nonfin_share_`x' == . ///
        & year > last_yr
    drop temp* last_3yr_mean last_yr nonmiss    
}
keep iso3_o year WRX_out_nonfin_share*
tempfile WRX_out_nonfin_share
save `WRX_out_nonfin_share', replace
restore

** step 0 : simple subtraction
foreach x in n_ent n_psn_emp rev {
    gen es_out_`x'_totXfin = es_out_`x'_tot - es_out_`x'_fin
}
local countVarList "es_out_n_ent_totXfin es_out_n_psn_emp_totXfin es_out_rev_totXfin"
egen nonmiss0 = rownonmiss(`countVarList')
gen exclude_fin_adj = "none" if nonmiss0 > 0
estpost tabulate nonmiss0, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_psn_emp and rev in Eurostat outward activities after step 0: simple subtraction") varlabels(`e(labels)')
** step 1: use nonfinancial share of total outward MP
merge m:1 iso3_o year using `WRX_out_nonfin_share', keep(master match) nogen
foreach x in n_ent n_psn_emp rev {
    replace es_out_`x'_totXfin = es_out_`x'_tot * WRX_out_nonfin_share_`x' ///
        if nonmiss0 == 0
}
egen nonmiss1 = rownonmiss(`countVarList')
estpost tabulate nonmiss1, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_psn_emp and rev in Eurostat outward activities after step 1: use nonfinancial share of total outward MP") varlabels(`e(labels)')
replace exclude_fin_adj = "total outward nonfinancial share" if nonmiss1 > nonmiss0
drop WRX_out_nonfin_share*
** step 2: use host country's share of nonfinancial output
ren iso3_d iso3
merge m:1 iso3 year using `nonfin_output_share', keep(master match) nogen
ren iso3 iso3_d
foreach x in n_ent n_psn_emp rev {
    replace es_out_`x'_totXfin = es_out_`x'_tot * nonfin_output_share ///
        if nonmiss1 == 0
}        
egen nonmiss2 = rownonmiss(`countVarList')
estpost tabulate nonmiss2, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_psn_emp and rev in Eurostat outward activities after step 2: use nonfinancial output share of the host country") varlabels(`e(labels)')
replace exclude_fin_adj = "host country nonfinancial output share" if nonmiss2 > nonmiss1
drop nonfin_output_share
** step 3: use home country's share of nonfinancial output
ren iso3_o iso3
merge m:1 iso3 year using `nonfin_output_share', keep(master match) nogen
ren iso3 iso3_o
foreach x in n_ent n_psn_emp rev {
    replace es_out_`x'_totXfin = es_out_`x'_tot * nonfin_output_share ///
        if nonmiss2 == 0
}        
egen nonmiss3 = rownonmiss(`countVarList')
estpost tabulate nonmiss3, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Number of nonmissing values among n_ent n_psn_emp and rev in Eurostat outward activities after step 3: use nonfinancial output share of the home country") varlabels(`e(labels)')
replace exclude_fin_adj = "home country nonfinancial output share" if nonmiss3 > nonmiss2
drop nonfin_output_share

estpost tabulate exclude_fin_adj year, missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Cases excluding financial sector in different steps (Eurostat outward)") varlabels(`e(labels)')
keep iso3* year es_out_*_totXfin
tempfile es_out
save `es_out', replace


****************************
** merge and update
****************************
use "processed_data/activity_OECD_eurostat_combined.dta", clear
foreach f in es_out oecd_in oecd_out {
    merge 1:1 iso3_o iso3_d year using ``f'', update
    estpost tabulate year _merge, elabels
    esttab using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
        title("Merge update with `f' - financial sector excluded") ///
        eqlabels(, lhs("year \ merge"))
    drop _merge

}

keep iso3* year *totXfin*
compress
save "processed_data/nonfin_OECD_eurostat_activity.dta", replace

log close _all
