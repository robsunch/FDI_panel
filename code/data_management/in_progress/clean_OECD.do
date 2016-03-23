***************************************
** this do-file cleans OECD_FDI_raw.dta
***************************************

log close _all
log using "$logDir/clean_OECD.smcl", replace

*********************
** standardize iso code
*********************
use iso3_* countryName_* using "processed_data/oecd_fdi.dta", clear
stack iso3_o countryName_o iso3_d countryName_d, into(iso3 countryName) clear
drop _stack
duplicates drop
ren countryName countryName_oecd
merge m:1 iso3 using "processed_data/isoStandard.dta"
sort _merge iso3
export excel using "processed_data/merge_country_lists.xlsx", sheet(check_oecd_iso3) sheetreplace firstrow(variables)
*** the only important discrepancy is Romania (ROM instead of ROU)


****************************************
*** construct exchange rates used for Euro area
*** and non Euro area
****************************************
*** EUR to USD exchange rate
import excel date euroExchRate using "source_data/Misc/DEXUSEU.xls", clear cellrange(a15:b30)
gen year = year(date)
tempfile euroExchRate
save `euroExchRate', replace

*** Euro fixed rates
import excel using "source_data/Misc/euroFixedRate.xlsx", clear firstrow
gen year_of_adoption = year(date_of_adoption)
tempfile euroFixedRates
save `euroFixedRates', replace

*** World Bank official exchange rates
import excel using "source_data/misc/pa.nus.fcrf_Indicator_en_excel_v2.xls", sheet("Data") cellrange(a3:bf261) firstrow clear
drop Indicator* CountryName
ren CountryCode iso3
quietly ds iso3, not
foreach x of varlist `r(varlist)' {
	local xlab : var label `x'
	ren `x' year`xlab'
	}
reshape long year, i(iso3) j(yr)
ren year exchRate_wdi
ren yr year

merge m:1 iso3 using `euroFixedRates', nogen
merge m:1 year using `euroExchRate', keep(master match) nogen

gen exchRate = exchRate_wdi
*** adjust for euro fixed rates before adoption
replace exchRate = exchRate / fixedRate if year<year_of_adoption & year_of_adoption<=2013
*** adjust for euro exchange rates after adoption
replace exchRate = 1/euroExchRate if year>=year_of_adoption & year_of_adoption<=2013 & missing(exchRate)

keep iso3 year exchRate fixedRate
tempfile exchRate
save `exchRate', replace

use "processed_data/oecd_fdi.dta", clear
foreach x in o d {
	replace iso3_`x' = "ROU" if iso3_`x'=="ROM"
	}

** reporting country is destination
ren iso3_d iso3
merge m:1 iso3 year using `exchRate', keep(master match) nogen

quietly count if ~missing(oecd_in_sales) & missing(exchRate)
if `r(N)' > 0 {
	display "some inward sales does not have corresponding exchange rate. check."
	error 1
	}
replace oecd_in_sales = oecd_in_sales / exchRate // millions of LCU to millions of USD
replace oecd_in_fin_sales = oecd_in_fin_sales / exchRate // millions of LCU to millions of USD
ren iso3 iso3_d
drop exchRate fixedRate

** reporting country is origin
ren iso3_o iso3
merge m:1 iso3 year using `exchRate', keep(master match) nogen
** adjust exchange rate for Slovenia
replace exchRate = exchRate * fixedRate if iso3=="SVN"

quietly count if ~missing(oecd_out_sales) & missing(exchRate)
if `r(N)' > 0 {
	display "some outward sales does not have corresponding exchange rate. check."
	error 1
	}

replace oecd_out_sales = oecd_out_sales / exchRate // millions of LCU to millions of USD
replace oecd_out_fin_sales = oecd_out_fin_sales / exchRate
ren iso3 iso3_o
drop exchRate fixedRate

*** rescale Germany's outward sales in 2007
replace oecd_out_sales = oecd_out_sales/1000 if year==2007 & iso3_o=="DEU"
replace oecd_out_fin_sales = oecd_out_fin_sales/1000 if year==2007 & iso3_o=="DEU"

save "processed_data/oecd_fdi.dta", replace

**************************************
**** adjust for financial sector FDI for sales ****
**************************************
use "processed_data/oecd_fdi.dta", clear

*** inward sales for year<=2007
preserve
keep if iso3_o=="WORLD" & year<=2007
keep oecd_in_fin_sales oecd_in_sales iso3_d year
gen world_in_nonfin_share_temp = 1 - oecd_in_fin_sales/oecd_in_sales
*** three year average
fillin iso3_d year
encode iso3_d, gen(id_iso3_d)
xtset id_iso3_d year
gen world_in_nonfin_share_1 = l.world_in_nonfin_share_temp
gen world_in_nonfin_share_3 = f.world_in_nonfin_share_temp
egen world_in_nonfin_share = rowmean(world_in_nonfin_share_1 world_in_nonfin_share_3 world_in_nonfin_share_temp)
keep iso3_d year world_in_nonfin_share
tempfile world_in_nonfin_share
save `world_in_nonfin_share', replace
restore

*** outward sales for all years
preserve
keep if iso3_d=="WORLD"
keep oecd_out_fin_sales oecd_out_sales iso3_o year
gen world_out_nonfin_share_temp = 1 - oecd_out_fin_sales/oecd_out_sales
merge 1:1 iso3_o year using `world_out_nonfin_share_after07', update nogen
*** three year average
fillin iso3_o year
encode iso3_o, gen(id_iso3_o)
xtset id_iso3_o year
gen world_out_nonfin_share_1 = l.world_out_nonfin_share_temp
gen world_out_nonfin_share_3 = f.world_out_nonfin_share_temp
egen world_out_nonfin_share = rowmean(world_out_nonfin_share_1 world_out_nonfin_share_3 world_out_nonfin_share_temp)
keep iso3_o year world_out_nonfin_share
tempfile world_out_nonfin_share
save `world_out_nonfin_share', replace
restore

*** first adjust for inward sales (before 2007)
merge m:1 iso3_d year using `world_in_nonfin_share', nogen
ren iso3_d iso3
merge m:1 iso3 year using "processed_data/nonfin_output_share.dta", keep(master match) ///
	keepusing(nonfin_output_share) nogen

display as text _n _n "Adjust inward MP sales before 2007 for the financial sector" _n
gen need_adjust = oecd_in_sales~=. & oecd_in_sales~=0 & year<=2007
count if need_adjust==1
display as text "Number of inward sales that need to be adjusted: " as result r(N)

gen oecd_in_sales_nonfin = oecd_in_sales
count if need_adjust==1 & ~missing(oecd_in_fin_sales)
display as text "Step 1: number of obs adjusted using bilateral FIN sales: " as result r(N)
replace oecd_in_sales_nonfin = oecd_in_sales_nonfin - oecd_in_fin_sales if need_adjust==1 & ~missing(oecd_in_fin_sales)
replace need_adjust=0 if need_adjust==1 & ~missing(oecd_in_fin_sales)

count if need_adjust==1 & ~missing(world_in_nonfin_share)
display as text "Step 2: number of obs adjusted using total inward World MP sales in financial sectors: " as result r(N)
replace oecd_in_sales_nonfin = oecd_in_sales_nonfin * world_in_nonfin_share if need_adjust==1 & ~missing(world_in_nonfin_share)
replace need_adjust=0 if need_adjust==1 & ~missing(world_in_nonfin_share)

count if need_adjust==1 & ~missing(nonfin_output_share)
display as text "Step 3: number of obs adjusted using share of nonfinancial output in the host country: " as result r(N)
replace oecd_in_sales_nonfin = oecd_in_sales_nonfin * nonfin_output_share if need_adjust==1 & ~missing(nonfin_output_share)
replace need_adjust=0 if need_adjust==1 & ~missing(nonfin_output_share)

quietly count if need_adjust==1
if `r(N)'>0 {
	display "still have inward sales that need to be adjusted. Check."
	error 1
	}
drop need_adjust nonfin_output_share world_in_nonfin_share oecd_in_fin_sales oecd_in_fin_sales_flag
ren iso3 iso3_d

*** next adjust for outward sales for all years
merge m:1 iso3_o year using `world_out_nonfin_share', nogen
ren iso3_o iso3
merge m:1 iso3 year using "processed_data/nonfin_output_share.dta", keep(master match) ///
	keepusing(nonfin_output_share) nogen

display as text _n _n "Adjust outward MP sales for the financial sector" _n
gen need_adjust = oecd_out_sales~=. & oecd_out_sales~=0
count if need_adjust==1
display as text "Number of outward sales that need to be adjusted: " as result r(N)

gen oecd_out_sales_nonfin = oecd_out_sales
count if need_adjust==1 & ~missing(oecd_out_fin_sales)
display as text "Step 1: number of obs adjusted using bilateral FIN sales: " as result r(N)
replace oecd_out_sales_nonfin = oecd_out_sales_nonfin - oecd_out_fin_sales if need_adjust==1 & ~missing(oecd_out_fin_sales)
replace need_adjust=0 if need_adjust==1 & ~missing(oecd_out_fin_sales)

count if need_adjust==1 & ~missing(world_out_nonfin_share)
display as text "Step 2: number of obs adjusted using total outward World MP sales in financial sectors: " as result r(N)
replace oecd_out_sales_nonfin = oecd_out_sales_nonfin * world_out_nonfin_share if need_adjust==1 & ~missing(world_out_nonfin_share)
replace need_adjust=0 if need_adjust==1 & ~missing(world_out_nonfin_share)

count if need_adjust==1 & ~missing(nonfin_output_share)
display as text "Step 3: number of obs adjusted using share of nonfinancial output in the host country: " as result r(N)
replace oecd_out_sales_nonfin = oecd_out_sales_nonfin * nonfin_output_share if need_adjust==1 & ~missing(nonfin_output_share)
replace need_adjust=0 if need_adjust==1 & ~missing(nonfin_output_share)

merge m:1 iso3 year using `nonfin_output_share_1012', update keep(1 3 4 5) nogen
count if need_adjust==1 & ~missing(nonfin_output_share)
display as text "Step 4: number of obs adjusted using share of nonfinancial output in the host country 2010-2012: " as result r(N)
replace oecd_out_sales_nonfin = oecd_out_sales_nonfin * nonfin_output_share if need_adjust==1 & ~missing(nonfin_output_share)
replace need_adjust=0 if need_adjust==1 & ~missing(nonfin_output_share)

quietly count if need_adjust==1
if `r(N)'>0 {
	display "still have outward sales that need to be adjusted. Check."
	error 1
	}
	
drop nonfin_output_share need_adjust world_out_nonfin_share oecd_out_fin_sales oecd_out_fin_sales_flag
ren iso3 iso3_o

file close myfile
gen share_in_sales = oecd_in_sales_nonfin / oecd_in_sales
gen share_out_sales = oecd_out_sales_nonfin / oecd_out_sales
estpost sum share_in_sales share_out_sales, detail
esttab . using "`outputPath'", append cells("mean sd count min p1 p10 p25 p50 p75 p90 p99 max") noobs ///
		title("Summary stats for nonfinancial sales in total sales")	
drop share_in_sales share_out_sales
		
compress
save "processed_data/oecd_fdi.dta", replace

log close _all
