******************************************************************
** this do-file imports OECD bilateral MNE activity data
** and OECD bilateral FDI stock and flow data
**
*******************************************************************

include project_paths
log using "${PATH_OUT_DATA}/log/`1'.log", replace

*** import ISIC 4 MNE activities by sector (total v.s. financial), world total (2007-2013)
insheet using "${PATH_IN_DATA}/FDI/OECD/AMNE_OUT4_world_total.csv", names clear
keep if var=="TUR" & ind=="C64-66"
ren cou iso3_o
keep iso3_o ind year value
ren value world_out_fin_sales
label var world_out_fin_sales "ISIC4 Sector C64-66, Mil LCU"
save "${PATH_OUT_DATA}/OECD/world_out_fin_sales_after07.dta", replace

insheet using "${PATH_IN_DATA}/FDI/OECD/AMNE_OUT4_world_total.csv", names clear
keep if var=="TUR" & ind=="C9999"
ren cou iso3_o
keep iso3_o ind year value
ren value world_out_tot_sales
label var world_out_tot_sales "ISIC4 Sector C9999, Mil LCU"
save "${PATH_OUT_DATA}/OECD/world_out_tot_sales_after07.dta", replace

*** import ISIC 4 MNE activities aggregates (2007-2013)
insheet using "${PATH_IN_DATA}/FDI/OECD/AMNE_OUT4_main_sectors.csv", names clear
keep if var=="TUR" & ind=="C9999" // TOTAL BUSINESS SECTOR (sec B to S excl. O)
ren (part partnercountry cou declaringcountry value flagcodes) ///
	(iso3_d countryName_d iso3_o countryName_o oecd_out4_sales oecd_out4_sales_flag)
keep iso3_d countryName_d iso3_o countryName_o oecd_out4_sales oecd_out4_sales_flag year
label var oecd_out4_sales "ISIC4 Sector C9999, Mil LCU"
tempfile out4
save `out4', replace

insheet using "${PATH_IN_DATA}/FDI/OECD/AMNE_IN4_main_sectors.csv", names clear
keep if var=="TUR" & ind=="C9994" // TOTAL ACTIVITY (sec B to N excl. K)
ren (part partnercountry cou declaringcountry value flagcodes) ///
	(iso3_o countryName_o iso3_d countryName_d oecd_in4_sales oecd_in4_sales_flag)
keep iso3_d countryName_d iso3_o countryName_o oecd_in4_sales oecd_in4_sales_flag year
label var oecd_in4_sales "ISIC4 Sector C9994, Mil LCU"

merge 1:1 iso3_o iso3_d countryName_o countryName_d year using `out4', nogen

tempfile amne_isic4
save `amne_isic4', replace

*** import ISIC3 MNE activities aggregates (1985-2013)
insheet using "${PATH_IN_DATA}/FDI/OECD/FATS_OUT3_main_sectors.csv", names clear
keep if var=="TURN" & inlist(serv,9999,6895)
ren (part partnercountries cou country value flagcodes) ///
	(iso3_d countryName_d iso3_o countryName_o oecd_out3_sales oecd_out3_sales_flag)
keep iso3_d countryName_d iso3_o countryName_o oecd_out3_sales oecd_out3_sales_flag year serv
reshape wide oecd_out3_sales oecd_out3_sales_flag, i(iso3_d countryName_d iso3_o countryName_o year) j(serv)
ren (oecd_out3_sales6895 oecd_out3_sales_flag6895 oecd_out3_sales9999 oecd_out3_sales_flag9999) ///
	(oecd_out3_fin_sales oecd_out3_fin_sales_flag oecd_out3_tot_sales oecd_out3_tot_sales_flag)
label var oecd_out3_tot_sales "ISIC3 Sector 9999, Mil LCU"
label var oecd_out3_fin_sales "ISIC3 Sector 6895, Mil LCU"   
tempfile fats_out3
save `fats_out3', replace

insheet using "${PATH_IN_DATA}/FDI/OECD/FATS_IN3_main_sectors.csv", names clear
keep if var=="TURN" & inlist(serv,9999,6895)
ren (part partnercountries cou country value flagcodes) ///
	(iso3_o countryName_o iso3_d countryName_d oecd_in3_sales oecd_in3_sales_flag)
keep iso3_d countryName_d iso3_o countryName_o oecd_in3_sales oecd_in3_sales_flag year serv
reshape wide oecd_in3_sales oecd_in3_sales_flag, i(iso3_d countryName_d iso3_o countryName_o year) j(serv)
ren (oecd_in3_sales6895 oecd_in3_sales_flag6895 oecd_in3_sales9999 oecd_in3_sales_flag9999) ///
	(oecd_in3_fin_sales oecd_in3_fin_sales_flag oecd_in3_tot_sales oecd_in3_tot_sales_flag)
label var oecd_in3_fin_sales "ISIC3 Sector 6895, Mil LCU"
label var oecd_in3_tot_sales "ISIC3 Sector 9999, Mil LCU"
merge 1:1 iso3_d countryName_d iso3_o countryName_o year using `fats_out3', nogen
tempfile fats_isic3
save `fats_isic3', replace

clear
use `amne_isic4'
merge 1:1 iso3_d countryName_d iso3_o countryName_o year using `fats_isic3'
tempfile oecd_sales
save `oecd_sales', replace
/*
keep if year>=2007 & year<=2009

display as text _n "Common part of ISIC3 and ISIC4 data: 2007-2009" _n
file close myfile
estpost tabulate _merge year
esttab using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
		title("Number of observations in each dataset") ///
		addnotes("_merge=1 if in ISIC4 data only and _merge=2 if in ISIC3 data only")
gen diff_in = oecd_in4_sales / oecd_in3_tot_sales - 1
gen diff_in_nonfin = oecd_in4_sales / (oecd_in3_tot_sales - oecd_in3_fin_sales) - 1
gen diff_out = oecd_out4_sales / oecd_out3_tot_sales - 1

estpost sum diff* if _merge==3, detail
esttab . using "`outputPath'", append cells("mean sd count min p1 p10 p25 p50 p75 p90 p99 max") noobs ///
		title("Summary stats for difference of MNE sales between ISIC3 and ISIC4 aggregates")
*/

*** combine the two data sets
*** before 2007 (including 2007) use isic 3
*** after 2007 use isic 4
use if year<=2007 using `fats_isic3', clear
ren (*in3* *out3*) (*in* *out*)
keep iso3* year countryName* *tot* *fin*
ren *_tot_sales* *_sales*
tempfile fats_isic3_before07
save `fats_isic3_before07', replace

use if year>=2008 using `amne_isic4', clear
ren (*in4* *out4*) (*in* *out*)
append using `fats_isic3_before07'
tempfile oecd_sales
save `oecd_sales', replace

*************
** FDI flows
*************
insheet using "${PATH_IN_DATA}/FDI/OECD/FDI_FLOW_PARTNER.csv", clear
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

insheet using "${PATH_IN_DATA}/FDI/OECD/FDI_FLOW_PARTNER.csv", clear
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
insheet using "${PATH_IN_DATA}/FDI/OECD/FDI_POSITION_PARTNER.csv", clear
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

insheet using "${PATH_IN_DATA}/FDI/OECD/FDI_POSITION_PARTNER.csv", clear
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
merge 1:1 iso3_o iso3_d year using `oecd_sales', update nogen

compress
save "${PATH_OUT_DATA}/OECD/OECD_FDI_raw.dta", replace



