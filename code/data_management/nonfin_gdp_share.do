*************************************
** this do file produces nonfinancial
** share in GDP using Eurostat data
*************************************

log close _all
log using "$logDir/nonfin_gdp_share.smcl", replace

insheet using "source_data/misc/nama_nace10_c.tsv", tab case clear

ds v1, not
foreach x in `r(varlist)' {
    local yr = `x'[1]
    ren `x' value`yr'
}
split v1, parse(",") gen(v1_)
drop v1
foreach x of varlist v1_1 - v1_3 {
    local xName = `x'[1]
    ren `x' `xName'
}
ren v1_4 geo
drop in 1

keep if indic_na == "B1G" // B1GM: Gross domestic product at market prices
keep if unit == "PC_TOT" // percent of total
keep if nace_r2 == "K" | nace_r2 == "TOTAL"
reshape long value, i(nace_r2 indic_na geo unit) j(year)
split value, parse(" ") // ignore flags
drop value value2
ren value1 value
replace value = "" if value == ":"
tab value if real(value)==.
destring value, replace

keep geo nace_r2 year value
reshape wide value, i(geo year) j(nace_r2) string
gen nonfin_gdp_share = valueTOTAL - valueK
ren geo iso2
replace iso2 = "GB" if iso2 == "UK"
replace iso2 = "GR" if iso2 == "EL"
merge m:1 iso2 using "processed_data/isoStandard.dta", keep(match) nogen

keep iso3 year nonfin_gdp_share
save "processed_data/nonfin_gdp_share.dta", replace
log close _all
