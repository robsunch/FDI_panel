***************************************************
** extrapolate MP shares to 2012 using information from 2008 to 
** 2012 (to smooth fluctuation)
**			
** input: processed_data/output_extrap_panel.dta
**		.../imputed_sales_time.dta
**
** output:
***************************************************

set emptycells drop
set matsize 10000

capture log close _all
!mkdir "Output/StataLog/clean_n_checking/"
log using "Output/StataLog/clean_n_checking/extrap_mp_sales.smcl", replace

local logPath = "Output/tables/clean_n_checking/extrap_mp_sales.txt"
file close _all
file open myfile using "`logPath'", write replace
file write myfile "This file records information related to the consolidation of the three datasets and the extrapolation of bilateral MP sales." _n _n

local excelPath = "Output/tables/clean_n_checking/extrap_mp_sales.xlsx"
capture rm "`excelPath'"

local startYear = 1995

**************************************
** merge three datasets, consolidate iso3 codes
**************************************
/*
use "processed_data/eurostat_fdi.dta", clear
foreach x in o d {
	ren iso2_`x' iso2
	merge m:1 iso2 using "processed_data/isoStandard.dta", keepusing(iso2 iso3) keep(match) nogen
	ren (iso2 iso3) (iso2_`x' iso3_`x')
	}
tempfile eurostat_fdi
save `eurostat_fdi', replace
	
use "processed_data/oecd_fdi.dta", clear
foreach x in o d {
	ren iso3_`x' iso3
	merge m:1 iso3 using "processed_data/isoStandard.dta", keepusing(iso3 iso2) keep(match) nogen
	ren (iso3 iso2) (iso3_`x' iso2_`x')
	}
tempfile oecd_fdi
save `oecd_fdi', replace

use "processed_data/UNCTAD_bilateral_FDI.dta", clear
foreach x in o d {
	ren iso3_`x' iso3
	merge m:1 iso3 using "processed_data/isoStandard.dta", keepusing(iso3 iso2) keep(match) nogen
	ren (iso3 iso2) (iso3_`x' iso2_`x')
	}
merge 1:1 iso3_o iso3_d year using `eurostat_fdi', nogen
merge 1:1 iso3_o iso3_d year using `oecd_fdi', nogen

drop *flow* iso2* countryName*
keep if year>=`startYear'
compress
save "Temp/consolidated_fdi.dta", replace
*/

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
local num_obs = 2012 - `startYear' + 1
set obs `num_obs'
gen year = _n + `startYear' - 1
tempfile years
save `years', replace

***********************************
** extrapolation
***********************************

*** Step b
use "Temp/partner_list.dta"
merge 1:1 iso3 using "processed_data/reporter_oecd_eurostat.dta", nogen
ren iso3 iso3_o
foreach x in oecd eurostat {
	ren `x'_report `x'_report_out
	replace `x'_report_out = 0 if missing(`x'_report_out)
	}
cross using "Temp/partner_list.dta"
merge m:1 iso3 using "processed_data/reporter_oecd_eurostat.dta", nogen
ren iso3 iso3_d
foreach x in oecd eurostat {
	ren `x'_report `x'_report_in
	replace `x'_report_in = 0 if missing(`x'_report_in)
	}	
	
egen id_pair = group(iso3_o iso3_d)
encode iso3_o, gen(id_iso3_o)
encode iso3_d, gen(id_iso3_d)

save "Temp/pair_id_list.dta", replace

cross using `years'

merge 1:1 iso3_o iso3_d year using "Temp/consolidated_fdi.dta", keep(master match) nogen
drop *flag* *notes* *source* // redundant variables for the current procedures

foreach y in in out {

	gen `y'_impute_step = 0
	
	foreach x in oecd eurostat {
		gen `x'_`y'_sales_1 = `x'_`y'_sales_nonfin
		gen `x'_`y'_sales_step = 0
		
		** Step b-1-1, b-2-1
		replace `x'_`y'_sales_step = 1 if missing(`x'_`y'_sales_nonfin) & ///
				( (`x'_`y'_stock<=0 & UNCTAD_`y'_stock<=0) | ///
				(`x'_`y'_stock<=0 & UNCTAD_`y'_stock==.) | ///
				(`x'_`y'_stock==. & UNCTAD_`y'_stock<=0) )
		replace `x'_`y'_sales_1 = 0 if missing(`x'_`y'_sales_nonfin) & ///
				( (`x'_`y'_stock<=0 & UNCTAD_`y'_stock<=0) | ///
				(`x'_`y'_stock<=0 & UNCTAD_`y'_stock==.) | ///
				(`x'_`y'_stock==. & UNCTAD_`y'_stock<=0) )
		
		** Step b-1-2, b-2-2
		gen log_`y'_sales_nonfin = log(`x'_`y'_sales_nonfin)
		gen log_`y'_stock = log(`x'_`y'_stock)
		egen num_positive = total(log_`y'_stock~=. & log_`y'_sales_nonfin~=. & year>=2001), by(id_pair)
		tab num_positive		

		if "`y'"=="in" {
			quietly reg log_`y'_sales_nonfin i.id_iso3_d#c.log_`y'_stock i.id_pair i.id_pair#c.year i.year ///
				if num_positive>=3
			}
		else if "`y'"=="out" {
			quietly reg log_`y'_sales_nonfin i.id_iso3_o#c.log_`y'_stock i.id_pair i.id_pair#c.year i.year ///
				if num_positive>=3
			}
		parmest, saving("Temp/impute_`x'_`y'_sales.dta", replace)
			
		display as text "log linear imputation for `x' `y' sales"
		display as text "r-square is " as result e(r2)
		display as text "adjusted r-square is " as result e(r2_a)
		display as text "Number of observations is " as result e(N)

		egen temp = min(year) if log_`y'_sales_nonfin~=., by(id_pair)
		egen begin_year = min(temp), by(id_pair)
		drop temp
		egen temp = max(year) if log_`y'_sales_nonfin~=., by(id_pair)
		egen end_year = min(temp), by(id_pair)
		drop temp		
		predict temp1 if num_positive==3 & year<=end_year+1 & year>=begin_year-1, xb
		predict temp2 if num_positive>=4 & year<=end_year+2 & year>=begin_year-2, xb
		gen temp = temp1
		replace temp = temp2 if missing(temp)
		corr temp log_`y'_sales_nonfin
		replace `x'_`y'_sales_step = 2 if missing(`x'_`y'_sales_1) & ~missing(temp)
		replace `x'_`y'_sales_1 = exp(temp) if missing(`x'_`y'_sales_1) & ~missing(temp)
		
		drop num_positive temp* log_`y'_sales_nonfin log_`y'_stock begin_year end_year
		
	}

	** Step b-1-3, b-2-3
	gen `y'_sales_step = 0
	replace `y'_sales_step = oecd_`y'_sales_step if oecd_report_`y'==1
	replace `y'_sales_step = eurostat_`y'_sales_step if eurostat_report_`y'==1 & oecd_report_`y'==0
	gen `y'_sales_1 = oecd_`y'_sales_1 if oecd_report_`y'==1
	replace `y'_sales_1 = eurostat_`y'_sales_1 if eurostat_report_`y'==1 & oecd_report_`y'==0

	** Step b-1-4, b-2-4
	replace `y'_sales_step = 4 if (`y'_sales_1==. & oecd_report_`y'==1 & eurostat_`y'_sales_1<=0) ///
		| (`y'_sales_1==. & eurostat_report_`y'==1 & oecd_`y'_sales_1<=0)
	replace `y'_sales_1 = 0 if (`y'_sales_1==. & oecd_report_`y'==1 & eurostat_`y'_sales_1<=0) ///
		| (`y'_sales_1==. & eurostat_report_`y'==1 & oecd_`y'_sales_1<=0)

	count if missing(`y'_sales_1)
	tab `y'_sales_step if ~missing(`y'_sales_1)

}

gen imputed_sales = out_sales_1
gen log_out_sales_1 = log(out_sales_1)
gen log_in_sales_1 = log(in_sales_1)
egen num_positive = total(log_in_sales_1~=. & log_out_sales_1~=.), by(year)
tab num_positive year
quietly regress log_out_sales_1 i.year i.year#c.log_in_sales_1
parmest, saving("Temp/impute_out_from_in_sales.dta", replace)

display as text "log linear imputation for outward sales (imputed) using inward sales (imputed)"
display as text "r-square is " as result e(r2)
display as text "adjusted r-square is " as result e(r2_a)
display as text "Number of observations is " as result e(N)

predict temp, xb

corr temp log_out_sales_1
replace imputed_sales = exp(temp) if missing(imputed_sales)

keep iso3_o iso3_d year imputed_sales out_sales_1 in_sales_1 *sales_nonfin
label var imputed_sales "final imputed sales step b-3"
label var out_sales_1 "imputed outward sales step b-2"
label var in_sales_1 "imputed inward sales step b-1"

save "processed_data/imputed_sales_time.dta", replace


/*
**********************************
** cross-sectional extrapolation
** Step c
**********************************
use "Temp/pair_id_list.dta", clear
cross using `years'
keep if year>=2008 & year<=2012
merge 1:1 iso3_o iso3_d year using "Temp/consolidated_fdi.dta", keep(master match) nogen
drop *flag* *notes* *source* // redundant variables for the current procedures

foreach y in out in {
	gen `y'_sales = oecd_`y'_sales_nonfin if oecd_report_`y' == 1
	replace `y'_sales = 0 if oecd_`y'_sales_nonfin ==. & oecd_report_`y' == 1 & ///
		( (oecd_`y'_stock<=0 & UNCTAD_`y'_stock<=0) | ///
		  (oecd_`y'_stock==. & UNCTAD_`y'_stock<=0) | ///
		  (oecd_`y'_stock<=0 & UNCTAD_`y'_stock==.) ) 
	replace `y'_sales = eurostat_`y'_sales_nonfin if eurostat_report_`y' == 1 ///
		& oecd_report_`y' == 0
	replace `y'_sales = 0 if `y'_sales==. & eurostat_report_`y' == 1 ///
		& oecd_report_`y' == 0 & ///
		( (eurostat_`y'_stock<=0 & UNCTAD_`y'_stock<=0) | ///
		  (eurostat_`y'_stock==. & UNCTAD_`y'_stock<=0) | ///
		  (eurostat_`y'_stock<=0 & UNCTAD_`y'_stock==.) ) 	
}
** extrapolate to some "2012-average"
gen log_out_sales = log(out_sales)
egen num_positive = total(log_out_sales~=.), by(id_pair)
quietly reg log_out_sales i.id_pair i.id_pair#c.year if num_positive>=3
predict log_out_sales_extrap if num_positive>=3, xb

** cross-section extrapolation
keep if year==2012
gen log_out_stock = log(oecd_out_stock) if oecd_report_out == 1
replace log_out_stock = log(eurostat_out_stock) if eurostat_report_out == 1 ///
	& oecd_report_out == 0
egen num_positive_o = total(log_out_stock~=. & log_out_sales_extrap~=.), by(iso3_o)
egen num_positive_d = total(log_out_stock~=. & log_out_sales_extrap~=.), by(iso3_d)

quietly reg log_out_sales_extrap log_out_stock i.id_iso3_o i.id_iso3_d if num_positive_o>=3 & num_positive_d>=3

display as text "log linear imputation for outward sales using outward stocks"
display as text "r-square is " as result e(r2)
display as text "adjusted r-square is " as result e(r2_a)
display as text "Number of observations is " as result e(N)
predict temp if num_positive_o>=3 & num_positive_d>=3, xb
corr temp log_out_sales_extrap log_out_sales

gen imputed_sales = exp(log_out_sales_extrap)
replace imputed_sales = exp(temp) if missing(imputed_sales)
	
save "processed_data/imputed_sales_cross_sec.dta", replace

*/

log close _all
