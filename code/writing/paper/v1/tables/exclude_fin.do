***********************************
** information related to adjustment of the 
** financial sector
***********************************

** prepare for country level nonfinancial output or gdp share
use "processed_data/nonfin_output_share.dta", clear
keep iso3
duplicates drop
tempfile nonfin_output_iso3
save `nonfin_output_iso3', replace

use "processed_data/nonfin_gdp_share.dta", clear
keep iso3
duplicates drop
merge 1:1 iso3 using `nonfin_output_iso3'

keep if _merge == 1
drop _merge

merge 1:m iso3 using "processed_data/nonfin_gdp_share.dta", nogen keep(match)
ren nonfin_gdp_share nonfin_output_share
replace nonfin_output_share = nonfin_output_share / 100
tempfile gdp_share_complement
save `gdp_share_complement',replace

use "processed_data/nonfin_output_share.dta", clear
local n_obs = _N + 1
set obs `n_obs'
replace year = 2012 in `n_obs'
fillin iso3 year
drop if missing(iso3)
encode iso3, gen(id_iso3)
egen last_yr = max(year*(nonfin_output_share < .)), by(iso3)
gen temp = nonfin_output_share if year == last_yr
egen last_yr_share = max(temp), by(iso3)
replace nonfin_output_share = last_yr_share if year>last_yr

merge 1:1 iso3 year using `gdp_share_complement'

keep iso3 year nonfin_output_share
tempfile nonfin_output_share
save `nonfin_output_share', replace

***********************
** OECD inward
***********************
use "processed_data/activity_OECD_eurostat_consol_emp.dta", replace
ren iso3_d iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(report_oecd_in) nogen
ren iso3 iso3_d
keep if report_oecd_in
keep oecd_in* iso3* year

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

** first keep observations with any information
ds *n_emp*tot* *n_ent*tot* *rev*tot*, has(type numeric)
egen anyData = rownonmiss(`r(varlist)')
drop if anyData == 0

** step 0 : simple subtraction
foreach x in n_ent n_emp rev {
    replace oecd_in_`x'_totXfin = oecd_in_`x'_tot - oecd_in_`x'_fin ///
        if oecd_in_`x'_totXfin == .
}
local countVarList "oecd_in_n_ent_totXfin oecd_in_n_emp_totXfin oecd_in_rev_totXfin"
egen nonmiss0 = rownonmiss(`countVarList')
gen exclude_fin_adj = 0 if nonmiss0 > 0

** step 1: use nonfinancial share of total inward MP
merge m:1 iso3_d year using `WRX_in_nonfin_share', keep(master match) nogen
foreach x in n_ent n_emp rev {
    replace oecd_in_`x'_totXfin = oecd_in_`x'_tot * WRX_in_nonfin_share_`x' ///
        if nonmiss0 == 0 & iso3_d~="WRT" // national total better use total nonfinancial output share
}
egen nonmiss1 = rownonmiss(`countVarList')
replace exclude_fin_adj = 1 if nonmiss1 > nonmiss0
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
replace exclude_fin_adj = 2 if nonmiss2 > nonmiss1
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
replace exclude_fin_adj = 3 if nonmiss3 > nonmiss2
drop nonfin_output_share

capture label drop lab_exclude_fin_adj
label define lab_exclude_fin_adj 0 "no adjustment needed" ///
    1 "adj using total inward nonfinancial share" ///
    2 "adj using host country nonfinancial output share" ///
    3 "adj using home country nonfinancial output share" 
label values exclude_fin_adj lab_exclude_fin_adj

estpost tabulate year exclude_fin_adj, missing
esttab . using "$tableDir/exclude_fin_oecd_in.tex", replace cell(b) unstack ///
    noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
    title("Cases excluding financial sector in different steps (OECD inward)") booktabs ///
    addnotes("0 - no adjustment needed" ///
        "1 - adj using total inward nonfinancial share" ///
        "2 - adj using host country nonfinancial output share" ///
        "3 - adj using home country nonfinancial output share")

**********************
** OECD outward
**********************
use "processed_data/activity_OECD_eurostat_consol_emp.dta", replace
ren iso3_o iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(report_oecd_out) nogen
ren iso3 iso3_o
keep if report_oecd_out
keep oecd_out* iso3* year    

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

** first keep observations with any information
ds *n_emp*tot* *n_ent*tot* *rev*tot*, has(type numeric)
egen anyData = rownonmiss(`r(varlist)')
drop if anyData == 0

** step 0 : simple subtraction
foreach x in n_ent n_emp rev {
    gen oecd_out_`x'_totXfin = oecd_out_`x'_tot - oecd_out_`x'_fin
}
local countVarList "oecd_out_n_ent_totXfin oecd_out_n_emp_totXfin oecd_out_rev_totXfin"
egen nonmiss0 = rownonmiss(`countVarList')
gen exclude_fin_adj = 0 if nonmiss0 > 0
** step 1: use nonfinancial share of total outward MP
merge m:1 iso3_o year using `WRX_out_nonfin_share', keep(master match) nogen
foreach x in n_ent n_emp rev {
    replace oecd_out_`x'_totXfin = oecd_out_`x'_tot * WRX_out_nonfin_share_`x' ///
        if nonmiss0 == 0
}
egen nonmiss1 = rownonmiss(`countVarList')
replace exclude_fin_adj = 1 if nonmiss1 > nonmiss0
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
replace exclude_fin_adj = 2 if nonmiss2 > nonmiss1
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
replace exclude_fin_adj = 3 if nonmiss3 > nonmiss2
drop nonfin_output_share

capture label drop lab_exclude_fin_adj
label define lab_exclude_fin_adj 0 "no adjustment needed" ///
    1 "adj using total outward nonfinancial share" ///
    2 "adj using host country nonfinancial output share" ///
    3 "adj using home country nonfinancial output share" 
label values exclude_fin_adj lab_exclude_fin_adj

estpost tabulate year exclude_fin_adj, missing
esttab . using "$tableDir/exclude_fin_oecd_out.tex", replace cell(b) unstack ///
    noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
    title("Cases excluding financial sector in different steps (OECD outward)") booktabs ///
    addnotes("0 - no adjustment needed" ///
        "1 - adj using total inward nonfinancial share" ///
        "2 - adj using host country nonfinancial output share" ///
        "3 - adj using home country nonfinancial output share")
*************************
** Eurostat outward
*************************
use "processed_data/activity_OECD_eurostat_consol_emp.dta", replace
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
foreach x in n_emp n_ent rev {
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

** first keep observations with any information
ds *n_emp*tot* *n_ent*tot* *rev*tot*, has(type numeric)
egen anyData = rownonmiss(`r(varlist)')
drop if anyData == 0

** step 0 : simple subtraction
foreach x in n_ent n_emp rev {
    gen es_out_`x'_totXfin = es_out_`x'_tot - es_out_`x'_fin
}
local countVarList "es_out_n_ent_totXfin es_out_n_emp_totXfin es_out_rev_totXfin"
egen nonmiss0 = rownonmiss(`countVarList')
gen exclude_fin_adj = 0 if nonmiss0 > 0
** step 1: use nonfinancial share of total outward MP
merge m:1 iso3_o year using `WRX_out_nonfin_share', keep(master match) nogen
foreach x in n_ent n_emp rev {
    replace es_out_`x'_totXfin = es_out_`x'_tot * WRX_out_nonfin_share_`x' ///
        if nonmiss0 == 0
}
egen nonmiss1 = rownonmiss(`countVarList')
replace exclude_fin_adj = 1 if nonmiss1 > nonmiss0
drop WRX_out_nonfin_share*
** step 2: use host country's share of nonfinancial output
ren iso3_d iso3
merge m:1 iso3 year using `nonfin_output_share', keep(master match) nogen
ren iso3 iso3_d
foreach x in n_ent n_emp rev {
    replace es_out_`x'_totXfin = es_out_`x'_tot * nonfin_output_share ///
        if nonmiss1 == 0
}        
egen nonmiss2 = rownonmiss(`countVarList')
replace exclude_fin_adj = 2 if nonmiss2 > nonmiss1
drop nonfin_output_share
** step 3: use home country's share of nonfinancial output
ren iso3_o iso3
merge m:1 iso3 year using `nonfin_output_share', keep(master match) nogen
ren iso3 iso3_o
foreach x in n_ent n_emp rev {
    replace es_out_`x'_totXfin = es_out_`x'_tot * nonfin_output_share ///
        if nonmiss2 == 0
}        
egen nonmiss3 = rownonmiss(`countVarList')
replace exclude_fin_adj = 3 if nonmiss3 > nonmiss2
drop nonfin_output_share

capture label drop lab_exclude_fin_adj
label define lab_exclude_fin_adj 0 "no adjustment needed" ///
    1 "adj using total outward nonfinancial share" ///
    2 "adj using host country nonfinancial output share" ///
    3 "adj using home country nonfinancial output share" 
label values exclude_fin_adj lab_exclude_fin_adj

estpost tabulate year exclude_fin_adj, missing
esttab . using "$tableDir/exclude_fin_es_out.tex", replace cell(b) unstack ///
    noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
    title("Cases excluding financial sector in different steps (Eurostat outward)") booktabs ///
    addnotes("0 - no adjustment needed" ///
        "1 - adj using total inward nonfinancial share" ///
        "2 - adj using host country nonfinancial output share" ///
        "3 - adj using home country nonfinancial output share")


