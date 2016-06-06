*******************************************
** this do-file compare UNCTAD country codes
** to standard codes
*******************************************

*** UNCTAD country codes
use ctyName_* iso3_* using "processed_data/UNCTAD_raw.dta", clear
stack iso3_o ctyName_o iso3_d ctyName_d, into(iso3 ctyName) clear
drop _stack
duplicates drop 

ren ctyName countryName_UNCTAD

preserve
keep if missing(iso3)
keep countryName_UNCTAD
duplicates drop
tempfile countryName_not_matched_iso3
save `countryName_not_matched_iso3', replace
restore

drop if missing(iso3)
preserve
merge m:1 iso3 using "processed_data/isoStandard.dta"
sort _merge iso3
export excel using "processed_data/isoStandard.xlsx", sheet("UNCTAD_input_match_iso3") sheetreplace firstrow(variables)
restore

merge m:1 countryName_UNCTAD using `countryName_not_matched_iso3', keep(using) nogen
drop iso3
ren countryName_UNCTAD countryName
merge 1:1 countryName using "processed_data/isoStandard.dta"
sort _merge iso3
export excel using "processed_data/isoStandard.xlsx", sheet("UNCTAD_input_no_match_iso3") sheetreplace firstrow(variables)

** manually adjust and export to sheet "UNCTAD_output_match_iso3", "UNCTAD_output_no_match_iso3"
    