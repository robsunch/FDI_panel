***************************************************
** consolidate FDI statistics from three sources
** and impute using cross-section variation
**
** input:	processed_data/eurostat_FDI.dta
**		.../oecd_fdi.dta
**		.../UNCTAD_bilateral_FDI.dta
**		source_data/Ramondo-appendix-data/appendix-dataset.dta
**
** output:	processed_data/reporter_oecd_eurostat.dta
**			processed_data/extrap_mp_sales_avg_begin_year_end_year.dta
**			Output/StataLog/clean_n_checking/extrap_mp_sales_cross_sec.smcl
**			Output/tables/clean_n_checking/extrap_mp_sales_cross_sec.xlsx
**
***************************************************

set emptycells drop
set matsize 10000

** args begin_year end_year before_after min_obs
local begin_year = 2006
local end_year = 2011
local before_after = 2
local min_obs = 4

capture log close _all
!mkdir "Output/StataLog/clean_n_checking/"
log using "Output/StataLog/clean_n_checking/extrap_mp_sales_cross_sec.smcl", replace

local logPath = "Output/tables/clean_n_checking/extrap_mp_sales_cross_sec.txt"
file close _all
file open myfile using "`logPath'", write replace
file write myfile "This file records information related to the consolidation of the three datasets and the extrapolation of bilateral MP sales." _n _n

local excelPath = "Output/tables/clean_n_checking/extrap_mp_sales_cross_sec.xlsx"
capture rm "`excelPath'"

**************************************
** merge three datasets, consolidate iso3 codes
** combine HK and CN in OECD and UNCTAD
**************************************
** Eurostat
use "processed_data/eurostat_fdi.dta", clear
foreach x in o d {
	ren iso2_`x' iso2
	merge m:1 iso2 using "processed_data/isoStandard.dta", keepusing(iso2 iso3) keep(match) nogen
	ren (iso2 iso3) (iso2_`x' iso3_`x')
	}
drop if iso3_o=="HKG" | iso3_d=="HKG"
drop iso2* *flag countryName*
tempfile eurostat_fdi
save `eurostat_fdi', replace

** OECD
use "processed_data/oecd_fdi.dta", clear
foreach x in o d {
	ren iso3_`x' iso3
	merge m:1 iso3 using "processed_data/isoStandard.dta", keepusing(iso3) keep(match) nogen
	ren iso3 iso3_`x'
	}	
drop *flag countryName*
	
** combine HKG and CHN
preserve
keep if inlist(iso3_o,"CHN","HKG") | inlist(iso3_d,"CHN","HKG")
gen iso3_o_temp = iso3_o
gen iso3_d_temp = iso3_d
replace iso3_o_temp = "CHN" if iso3_o_temp=="HKG"
replace iso3_d_temp = "CHN" if iso3_d_temp=="HKG"
drop if iso3_o_temp=="CHN" & iso3_d_temp=="CHN"
quietly ds iso3_* year, not
foreach x in `r(varlist)' {
	gen nmiss1_`x' = (iso3_o=="CHN" | iso3_d=="CHN") & `x'~=.
	gen nmiss2_`x' = `x'~=.
	}
	
quietly ds iso3_* year, not
collapse (sum) `r(varlist)', by(iso3_o_temp iso3_d_temp year)
quietly ds iso3_* year nmiss*, not
foreach x in `r(varlist)' {
	replace `x' = . if nmiss2_`x'==0 | nmiss1_`x'==0
	}
drop nmiss*
ren (iso3_o_temp iso3_d_temp) (iso3_o iso3_d)
tempfile chn_fdi
save `chn_fdi', replace
restore

drop if inlist(iso3_o,"CHN","HKG") | inlist(iso3_d,"CHN","HKG")
append using `chn_fdi'

tempfile oecd_fdi
save `oecd_fdi', replace

** UNCTAD data
use "processed_data/UNCTAD_bilateral_FDI.dta", clear
foreach x in o d {
	ren iso3_`x' iso3
	merge m:1 iso3 using "processed_data/isoStandard.dta", keepusing(iso3) keep(match) nogen
	ren iso3 iso3_`x'
	}
drop *notes *source

** combine HKG and CHN
preserve
keep if inlist(iso3_o,"CHN","HKG") | inlist(iso3_d,"CHN","HKG")
gen iso3_o_temp = iso3_o
gen iso3_d_temp = iso3_d
replace iso3_o_temp = "CHN" if iso3_o_temp=="HKG"
replace iso3_d_temp = "CHN" if iso3_d_temp=="HKG"
drop if iso3_o_temp=="CHN" & iso3_d_temp=="CHN"
quietly ds iso3_* year, not
foreach x in `r(varlist)' {
	gen nmiss1_`x' = (iso3_o=="CHN" | iso3_d=="CHN") & `x'~=.
	gen nmiss2_`x' = `x'~=.
	}
	
quietly ds iso3_* year, not
collapse (sum) `r(varlist)', by(iso3_o_temp iso3_d_temp year)
quietly ds iso3_* year nmiss*, not
foreach x in `r(varlist)' {
	replace `x' = . if nmiss2_`x'==0 | nmiss1_`x'==0
	}
drop nmiss*
ren (iso3_o_temp iso3_d_temp) (iso3_o iso3_d)
tempfile chn_fdi
save `chn_fdi', replace
restore

drop if inlist(iso3_o,"CHN","HKG") | inlist(iso3_d,"CHN","HKG")
append using `chn_fdi'

	
*** merge with OECD and Eurostat data	
merge 1:1 iso3_o iso3_d year using `eurostat_fdi', nogen
merge 1:1 iso3_o iso3_d year using `oecd_fdi', nogen

compress
save "Temp/consolidated_fdi.dta", replace


********************************************
** construct the sample of reporting countries
********************************************
use "processed_data/oecd_fdi.dta", clear
egen nonmiss_in = rownonmiss(oecd_in*sales oecd_in*sales)
keep if nonmiss_in > 0
keep iso3_d
ren iso3_d iso3
tempfile oecd_report1
save `oecd_report1', replace

use "processed_data/oecd_fdi.dta", clear
egen nonmiss_out = rownonmiss(oecd_out*sales oecd_out*sales)
keep if nonmiss_out > 0
keep iso3_o
ren iso3_o iso3

append using `oecd_report1'
duplicates drop
replace iso3 = "ROU" if iso3=="ROM"
tempfile oecd_report
save `oecd_report', replace

use if ~missing(eurostat_in_sales_nonfin) using "processed_data/eurostat_fdi.dta", clear
keep iso2_d
ren iso2_d iso2
tempfile eurostat_report1
save `eurostat_report1', replace

use if ~missing(eurostat_out_sales) using "processed_data/eurostat_fdi.dta", clear
keep iso2_o
ren iso2_o iso2
append using `eurostat_report1'
duplicates drop
merge 1:1 iso2 using "processed_data/isoStandard.dta", keep(match) nogen
keep iso3
tempfile eurostat_report
save `eurostat_report', replace

append using `oecd_report'
duplicates drop

merge 1:1 iso3 using `oecd_report'
gen oecd_report = _merge==3
drop _merge

merge 1:1 iso3 using `eurostat_report'
gen eurostat_report= _merge==3
drop _merge

export excel using "`excelPath'", sheetreplace sheet("report") firstrow(variables)
save "processed_data/reporter_oecd_eurostat.dta", replace


*** generate a list of partners
use iso3_o iso3_d using "Temp/consolidated_fdi.dta", clear
stack iso3_o iso3_d, into(iso3) clear
drop _stack
duplicates drop
save "Temp/partner_list.dta", replace


*** generate a list of years
clear
local num_obs = `end_year' - `begin_year' + 1 + 2 * `before_after'
set obs `num_obs'
gen year = `begin_year' - 1 + _n
tempfile years
save `years', replace

*** construct the panel
use "Temp/pair_id_list.dta", clear
cross using `years'
keep if year>=`begin_year'-`before_after' & year<=`end_year'+`before_after'
merge 1:1 iso3_o iso3_d year using "Temp/consolidated_fdi.dta", keep(master match) nogen


**********************************
** step 1: average between begin_year
** 		   and end_year
**
**********************************

** step 1-a: identify zeros, first stocks, then sales
foreach y in eurostat oecd UNCTAD {
foreach x in in out { 
	** number of positive stocks
	egen Pos_`y'_`x'_stock = total(year>=`begin_year' & year<=`end_year' ///
				& (`y'_`x'_stock>0 & `y'_`x'_stock<.)), by(id_pair)
	** number of nonpositive stocks
	egen NonPos_`y'_`x'_stock = total(year>=`begin_year' & year<=`end_year' ///
				& `y'_`x'_stock<=0), by(id_pair)
	gen zero_stock_`y'_`x' = Pos_`y'_`x'_stock==0 & NonPos_`y'_`x'_stock>0
	replace `y'_`x'_stock = 0 if zero_stock_`y'_`x'
	
	}
	}
	
foreach y in eurostat oecd {
foreach x in in out {

	** number of nonmissing and nonzero sales ( positive or negative values)
	egen PosNeg_`y'_`x'_sales = total(year>=`begin_year' & year<=`end_year' ///
				& (`y'_`x'_sales_nonfin~=0 & `y'_`x'_sales_nonfin~=.)), by(id_pair)
	gen zero_sales_`y'_`x' = (zero_stock_`y'_`x' | zero_stock_UNCTAD_`x') ///
			& PosNeg_`y'_`x'_sales == 0
	replace `y'_`x'_sales_nonfin = 0 if zero_sales_`y'_`x'
	
	}
	}

** step 1-b: 	extrapolate over time assuming constant growth

** divide sample into subsamples to speed up regression
quietly sum id_pair
local num_firm_per_group = 10000
local num_group = ceil(`r(N)'/`num_firm_per_group')

foreach y in eurostat oecd UNCTAD {
foreach x in in out {

	** extrapolate stocks
	gen log_stock = log(`y'_`x'_stock)
	egen num_nonmiss_stock = total(log_stock~=.), by(id_pair)
	gen temp_stock = .
	forvalues i = 1/`num_group' {
		quietly count if num_nonmiss_stock>=`min_obs' & ///
			id_pair > (`i'-1)*`num_firm_per_group' & id_pair <= `i'*`num_firm_per_group' ///
			& ~missing(log_stock)
		if `r(N)'==0 {
			disp as text "No observation for group `i'. Skip."
			}
		else {
			disp as text "Extrapolating `y'_`x'_stock using constant growth for group `i'."
			quietly reg log_stock i.id_pair i.id_pair#c.year if num_nonmiss_stock>=`min_obs' & ///
				id_pair > (`i'-1)*`num_firm_per_group' & id_pair <= `i'*`num_firm_per_group'
			predict temp if num_nonmiss_stock>=`min_obs' & ///
				id_pair > (`i'-1)*`num_firm_per_group' & id_pair <= `i'*`num_firm_per_group' ///
				, xb
			replace temp_stock = temp if temp_stock==.
			drop temp
			}
		}
		
	corr temp_stock log_stock
	gen impute_`y'_`x'_stock_1b = temp_stock~=. & `y'_`x'_stock==. ///
		& year>=`begin_year' & year<=`end_year'		
	replace `y'_`x'_stock = exp(temp_stock) if impute_`y'_`x'_stock_1b
	gen miss_`y'_`x'_stock = missing(`y'_`x'_stock) // still missing after extrapolation 1b

	drop log_stock num_nonmiss* temp*
	
	}
	}

foreach y in eurostat oecd {
foreach x in in out {
	
	** extrapolate sales
	gen log_sales = log(`y'_`x'_sales_nonfin)
	egen num_nonmiss_sales = total(log_sales~=.), by(id_pair)
	gen temp_sales = .
	forvalues i = 1/`num_group' {
		quietly count if num_nonmiss_sales>=`min_obs' & ///
			id_pair > (`i'-1)*`num_firm_per_group' & id_pair <= `i'*`num_firm_per_group' ///
			& ~missing(log_sales)
		if `r(N)'==0 {
			disp as text "No observation for group `i'. Skip."
			}
		else {
			disp as text "Extrapolating `y'_`x'_sales using constant growth for group `i'."
			quietly reg log_sales i.id_pair i.id_pair#c.year if num_nonmiss_sales>=`min_obs' & ///
				id_pair > (`i'-1)*`num_firm_per_group' & id_pair <= `i'*`num_firm_per_group'
			predict temp if num_nonmiss_sales>=`min_obs' & ///
				id_pair > (`i'-1)*`num_firm_per_group' & id_pair <= `i'*`num_firm_per_group' ///
				, xb
			replace temp_sales = temp if temp~=.
			drop temp
			}
		}
	corr temp_sales log_sales
	gen impute_`y'_`x'_sales_1b = temp_sales~=. & `y'_`x'_sales_nonfin==. ///
		& year>=`begin_year' & year<=`end_year'	
	replace `y'_`x'_sales_nonfin = exp(temp_sales) if impute_`y'_`x'_sales_1b
	gen miss_`y'_`x'_sales = missing(`y'_`x'_sales_nonfin) // still missing after extrapolation 1b
	
	drop log_sales num_nonmiss* temp*
	
	}
	}
	
** step 1-c: average
keep if year>=`begin_year' & year<=`end_year'
collapse (mean) eurostat*_stock oecd*_stock UNCTAD*_stock *_sales_nonfin (sum) impute_* miss_* (first) *report* zero_sales_*, by(id_pair iso3_o iso3_d)

** avoid treating missing as zero in collapse (mean)
foreach y in eurostat oecd UNCTAD {
foreach x in in out {
	replace `y'_`x'_stock = . if miss_`y'_`x'_stock>0
	if "`y'"~="UNCTAD" {
		replace `y'_`x'_sales_nonfin = . if miss_`y'_`x'_sales>0
	}
}
}
	
save "Temp/extrap_mp_sales_cs_step1.dta", replace // for testing only

****************************************
** step 2: impute missing outward variables using
** inward variables 
****************************************
local begin_year = 2006
local end_year = 2011
local before_after = 2
local min_obs = 4		
local logPath = "Output/tables/clean_n_checking/extrap_mp_sales_cross_sec.txt"	
file open myfile using "`logPath'", write append
use "Temp/extrap_mp_sales_cs_step1.dta", clear

** step 2-a: combine OECD and Eurostat data (and UNCTAD for FDI stock)
foreach x in in out {
	gen report_`x' = oecd_report_`x'==1 | eurostat_report_`x'==1
	
	gen `x'_stock = oecd_`x'_stock if oecd_report_`x'==1
	replace `x'_stock = eurostat_`x'_stock if eurostat_report_`x'==1 & oecd_report_`x'==0
	replace `x'_stock = UNCTAD_`x'_stock if report_`x' == 0
	
	gen `x'_sales = oecd_`x'_sales_nonfin if oecd_report_`x'==1
	replace `x'_sales = eurostat_`x'_sales_nonfin if eurostat_report_`x'==1 & oecd_report_`x'==0
	
	}

** step 2-b: extrapolate outward variables using inward variables
encode iso3_o, gen(id_iso3_o)
encode iso3_d, gen(id_iso3_d)

eststo clear
foreach y in sales stock {

	gen impute_`y'_2b = 0
	
	gen log_in_`y' = log(in_`y')
	gen log_out_`y' = log(out_`y')
	egen num_desti = total(log_in_`y'~=. & log_out_`y'~=.), by(iso3_o)
	
	quietly eststo: reg log_out_`y' log_in_`y' i.id_iso3_o if num_desti>=3 & report_out==1, ro
	predict temp1_log_out_`y' if num_desti>=3 & report_out==1, xb
	corr temp1_log_out_`y' log_out_`y'
	replace impute_`y'_2b = 1 if temp1_log_out_`y'~=. & report_out==1
	replace out_`y' = exp(temp1_log_out_`y') if temp1_log_out_`y'~=. & report_out==1
	
	egen num_source = total(log_in_`y'~=. & log_out_`y'~=.), by(iso3_d)
	quietly eststo: reg log_out_`y' log_in_`y' i.id_iso3_d if num_source>=3 & report_in==1, ro
	predict temp2_log_out_`y' if num_source>=3 & report_in==1, xb
	corr temp2_log_out_`y' log_out_`y'
	replace impute_`y'_2b = 1 if temp2_log_out_`y'~=. & report_in==1 & report_out==0
	replace out_`y' = exp(temp2_log_out_`y') if temp2_log_out_`y'~=. & report_in==1 & report_out==0
	
	drop log_* temp* num_*
	
}	


file write myfile _n
file close myfile
esttab * using "`logPath'", append se r2 nogaps drop(*id_iso3*) ///
	mtitles("report_out_sales" "report_in_sales" "report_out_stock" "report_in_stock") ///
	title("Extrapolating inward variables to outward")

**************************************
** extrapolate sales using stocks
**************************************
gen log_stock = log(out_stock)
gen log_sales = log(out_sales)
egen num_desti = total(log_stock~=. & log_sales~=.), by(iso3_o)
egen num_source = total(log_stock~=. & log_sales~=.), by(iso3_d)

eststo clear
quietly eststo: reg log_sales log_stock i.id_iso3_o i.id_iso3_d if num_desti>=3 & num_source>=3, ro
esttab * using "`logPath'", append se r2 nogaps drop(*id_iso3*) ///
	title("Extrapolating stock to sales")

predict temp_log_sales if num_desti>=3 & num_source>=3, xb
corr temp_log_sales log_sales
gen imputed_sales_cross_sec = out_sales
gen impute_sales_3 = temp_log_sales~=. & imputed_sales_cross_sec==.
replace imputed_sales_cross_sec = exp(temp_log_sales) if impute_sales_3==1

drop temp* log*

compress
save "processed_data/extrap_mp_sales_cs_`begin_year'_`end_year'.dta", replace

log close _all

