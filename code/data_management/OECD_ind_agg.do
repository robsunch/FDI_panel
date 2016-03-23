*****************************************
** import OECD industry aggregate MNE activity
** and FDI flows/stocks
**
*****************************************
log close _all
log using "$logDir/OECD_ind_agg.smcl", replace

local data_in = "processed_data/temp/OECD"
local data_out = "processed_data/OECD"
capture mkdir "`data_out'"

foreach x in FATS AMNE {
    import excel using "processed_data/select_var.xlsx", ///
        sheet("OECD_`x'_output") firstrow clear
    tempfile selected_var_`x'
    save `selected_var_`x'', replace
}

*** ISIC 4 outward MNE activities (total and financial), world total (2007-2013)
use if inlist(ind,"C64-66","C9999") using "`data_in'/AMNE_OUT4_world_total.dta", clear
merge m:1 var using `selected_var_AMNE', keep(match) keepusing(varName) nogen
drop var ind_des var_des flag_des
reshape wide value flag, i(iso3_o iso3_d year ind) j(varName) string
ren (value* flag*) (* flag_*)
ds iso3_* countryName_* year ind, not
replace ind = "C6466" if ind=="C64-66"
replace ind = "_" + ind
reshape wide `r(varlist)', i(iso3_o iso3_d year) j(ind) string
compress
save "`data_out'/AMNE_OUT4_world_tot_fin.dta", replace

*** ISIC 4 outward MNE activities aggregates (2007-2013), bilateral
use if inlist(ind,"C9999") using "`data_in'/AMNE_OUT4_main_sectors.dta", clear
merge m:1 var using `selected_var_AMNE', keep(match) keepusing(varName) nogen
drop var var_des ind ind_des flag_des
reshape wide value flag, i(iso3_o iso3_d year) j(varName) string
ren (value* flag*) (*_C9999 flag_*_C9999)
compress
save "`data_out'/AMNE_OUT4_bilat_tot.dta", replace

*** ISIC 4 inward MNE activities aggregates (2007-2013), bilateral
use if inlist(ind,"C9994","C9999") using "`data_in'/AMNE_IN4_main_sectors.dta", clear
merge m:1 var using `selected_var_AMNE', keep(match) keepusing(varName) nogen
drop var var_des ind_des flag_des
reshape wide value flag, i(iso3_o iso3_d year ind) j(varName) string
ren (value* flag*) (* flag_*)
ds iso3_* countryName_* year ind, not
replace ind = "_" + ind
reshape wide `r(varlist)', i(iso3_o iso3_d year) j(ind) string
compress
save "`data_out'/AMNE_IN4_bilat_tot.dta", replace

*** ISIC 3 outward FATS (1995-2009), bilateral
use if inlist(ind,6895,9999) using "`data_in'/FATS_OUT3_main_sectors.dta", replace
merge m:1 var using `selected_var_FATS', keep(match) keepusing(varName) nogen
gen suffix_ind = "_ind" + string(ind)
drop var var_des ind ind_des flag_des
reshape wide value flag, i(iso3_o iso3_d year suffix_ind) j(varName) string
ren (value* flag*) (* flag_*)
ds iso3_* countryName_* year suffix_ind, not
reshape wide `r(varlist)', i(iso3_* countryName_* year) j(suffix_ind) string
compress
save "`data_out'/FATS_OUT3_bilat_tot_fin.dta", replace

*** ISIC 3 inward FATS, bilateral
use if inlist(ind,6895,9994,9999) using "`data_in'/FATS_in3_main_sectors.dta", replace
merge m:1 var using `selected_var_FATS', keep(match) keepusing(varName) nogen
gen suffix_ind = "_ind" + string(ind)
drop var var_des ind ind_des flag_des
reshape wide value flag, i(iso3_o iso3_d year suffix_ind) j(varName) string
ren (value* flag*) (* flag_*)
ds iso3_* countryName_* year suffix_ind, not
reshape wide `r(varlist)', i(iso3_* countryName_* year) j(suffix_ind) string
compress
save "`data_out'/FATS_IN3_bilat_tot_fin.dta", replace

*************
** FDI flows
*************
insheet using "source_data/OECD/FDI_FLOW_PARTNER.csv", clear
keep if flow=="IN" // FDI inflows
drop typeoffdi currency

** drop LCU values, but first check completeness
preserve
reshape wide value flags flagcodes, i(pc partnercountry cou reportingcountry year) j(cur) string
codebook value*
quietly count if missing(valueUSD) & ~missing(valueSUB)
display as text "Number of inflows with missing USD value but nonmissing LCU value: " as result r(N)
quietly count if ~missing(valueUSD) & missing(valueSUB)
display as text "Number of inflows with missing LCU value but nonmissing USD value: " as result r(N)
restore
keep if cur=="USD"
ren (value flagcodes cou reportingcountry pc partnercountry) (oecd_in_flow oecd_in_flow_flag iso3_d countryName_d iso3_o countryName_o)
keep oecd_in_flow oecd_in_flow_flag iso3_d iso3_o year countryName_o countryName_d
label var oecd_in_flow "Mil USD"
tempfile oecd_in_flow
save `oecd_in_flow', replace

insheet using "source_data/OECD/FDI_FLOW_PARTNER.csv", clear
keep if flow=="OUT" // FDI outflows
drop typeoffdi currency
preserve
reshape wide value flags flagcodes, i(pc partnercountry cou reportingcountry year) j(cur) string
codebook value*
quietly count if missing(valueUSD) & ~missing(valueSUB)
display as text "Number of outflows with missing USD value but nonmissing LCU value: " as result r(N)
quietly count if ~missing(valueUSD) & missing(valueSUB)
display as text "Number of outflows with missing LCU value but nonmissing USD value: " as result r(N)
restore
keep if cur=="USD"
ren (value flagcodes cou reportingcountry pc partnercountry) (oecd_out_flow oecd_out_flow_flag iso3_o countryName_o iso3_d countryName_d)
keep oecd_out_flow oecd_out_flow_flag iso3_d iso3_o year countryName_o countryName_d
label var oecd_out_flow "Mil USD"
merge 1:1 iso3_o iso3_d year using `oecd_in_flow', nogen
tempfile oecd_flow
save `oecd_flow', replace

*****************
** FDI stocks
*****************
insheet using "source_data/OECD/FDI_POSITION_PARTNER.csv", clear
keep if flow=="IN" // inward FDI stocks
drop typeoffdi currency
preserve
reshape wide value flags flagcodes, i(pc partnercountry cou reportingcountry year) j(cur) string
codebook value*
quietly count if missing(valueUSD) & ~missing(valueSUB)
display as text "Number of instocks with missing USD value but nonmissing LCU value: " as result r(N)
quietly count if ~missing(valueUSD) & missing(valueSUB)
display as text "Number of instocks with missing LCU value but nonmissing USD value: " as result r(N)
restore
keep if cur=="USD"
ren (value flagcodes cou reportingcountry pc partnercountry) (oecd_in_stock oecd_in_stock_flag iso3_d countryName_d iso3_o countryName_o)
keep oecd_in_stock oecd_in_stock_flag iso3_d iso3_o year countryName_o countryName_d
tempfile oecd_in_stock
label var oecd_in_stock "Mil USD"
save `oecd_in_stock', replace

insheet using "source_data/OECD/FDI_POSITION_PARTNER.csv", clear
keep if flow=="OUT" // outward FDI stocks
drop typeoffdi currency
preserve
reshape wide value flags flagcodes, i(pc partnercountry cou reportingcountry year) j(cur) string
codebook value*
quietly count if missing(valueUSD) & ~missing(valueSUB)
display as text "Number of outstocks with missing USD value but nonmissing LCU value: " as result r(N)
quietly count if ~missing(valueUSD) & missing(valueSUB)
display as text "Number of outstocks with missing LCU value but nonmissing USD value: " as result r(N)
restore
keep if cur=="USD"
ren (value flagcodes cou reportingcountry pc partnercountry) (oecd_out_stock oecd_out_stock_flag iso3_o countryName_o iso3_d countryName_d)
keep oecd_out_stock oecd_out_stock_flag iso3_d iso3_o year countryName_o countryName_d
label var oecd_out_stock "Mil USD"
merge 1:1 iso3_o iso3_d year using `oecd_in_stock', nogen
tempfile oecd_stock
save `oecd_stock', replace

**************************************
** merge all OECD FDI related datasets
**************************************
use `oecd_stock', clear
merge 1:1 iso3_o iso3_d year using `oecd_flow', update nogen
compress
save "`data_out'/stock_flow.dta", replace

log close _all

