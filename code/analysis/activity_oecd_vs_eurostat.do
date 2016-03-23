***************************************************
** compare OECD and Eurostat statistics on MNE activities
**
** coverage and differences in the common sample
**
***************************************************

log close _all
log using "$logDir/activity_oecd_vs_eurostat.smcl", replace

local outputPath = "$tableDir/activity_oecd_vs_eurostat.csv"
capture rm "`outputPath'"


**********************************
*** inward activities coverage
**********************************
use "processed_data/activity_OECD_eurostat_combined.dta", clear
** drop iso3_d not among reporting countries in Eurostat or OECD
ren iso3_d iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", keep(match) keepusing(iso3) nogen
ren iso3 iso3_d

preserve
quietly ds *out* *flag* iso3* year, not
collapse (sum) `r(varlist)', by(iso3_d year)
tempfile agg_in
save `agg_in', replace
restore

foreach x of varlist *in*flag* {
    gen temp = ~missing(`x')
    drop `x'
    ren temp `x'
}

quietly ds *out* *flag* iso3* year, not
collapse (count) `r(varlist)' (sum) *_in*flag*, by(iso3_d year)
tempfile nonmiss_in
save `nonmiss_in', replace

use `nonmiss_in', clear



log close _all