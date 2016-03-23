*****************************************************
** this do-file imports Eurostat bilateral affiliate sales, 
** FDI flows and FDI stocks data
*****************************************************

include project_paths
log using "${PATH_OUT_DATA}/log/`1'.log", replace

!mkdir "${PATH_OUT_DATA}/Temp"


*** EUR to USD exchange rate
import excel date exchRate using "SourceData/Misc/DEXUSEU.xls", clear cellrange(a15:b30)
gen year = year(date)
tempfile euroExchRate
save `euroExchRate', replace

*** compare year 2008
use `eurostat_stock_flow2_r1', clear
foreach x of varlist eurostat_*_stock eurostat_*_flow {
	ren `x' `x'_r1
	}
merge 1:1 iso2_o iso2_d year using `eurostat_stock_flow2_r2'
foreach x of varlist eurostat_*_stock eurostat_*_flow {
	gen diff_`x' = (`x' - `x'_r1) / `x'_r1 if _merge==3
	}

file close myfile
estpost sum diff_*, detail
esttab . using "`outputPath'", append cells("mean sd count min p1 p10 p50 p90 p99 max") noobs ///
	title("Summary stats for difference of total FDI in 2008 for NACE R1 and R2")	

estpost tabulate _merge if inlist(year,2008,2009)
esttab . using "`outputPath'", append varlabels(`e(labels)') nogaps noobs nonotes noabbrev ///
	addnotes("_merge=1 if in R1 only, _merge=2 if in R2 only") ///
	title("Coverage of bilateral FDI in the two versions of data in 2008 and 2009")	

*** use R2 if possible, update with R1 for the year 2008 and 2009
use `eurostat_stock_flow2_r2', clear
merge 1:1 iso2_o iso2_d year using `eurostat_stock_flow2_r1', update nogen
tempfile eurostat_stock_flow2
save `eurostat_stock_flow2', replace
	
*****************************
** merge flows and stocks from TEC
** and BOP tables
*****************************

** check whether the common part of the tec and BOP tables
** are exactly the same

use `eurostat_stock_flow2', clear
foreach x in out in {
	foreach y in stock flow {
		ren eurostat_`x'_`y' eurostat_`x'_`y'2
		}
	}
tempfile to_merge_eurostat2
save `to_merge_eurostat2', replace

use `eurostat_stock_flow1', clear
merge 1:1 iso2_d iso2_o year using `to_merge_eurostat2'
foreach x in out in {
	foreach y in stock flow {
		gen diff_`x'_`y' = eurostat_`x'_`y'2 / eurostat_`x'_`y' - 1
		}
	}
estpost sum diff_*, detail
esttab . using "`outputPath'", append cells("mean sd count min p1 p10 p50 p90 p99 max") noobs ///
	title("Summary stats for difference of total FDI in the TEC and BOP tables")

estpost tabulate year _merge
esttab . using "`outputPath'", append varlabels(`e(labels)') cell(b) ///
	unstack nonumber nogaps noobs nonotes noabbrev ///
	addnotes("_merge=1 if in TEC only, _merge=2 if in BOP only") ///
	title("Coverage of bilateral FDI in the TEC and BOP tables")		


****** use BOP tables whenever possible
****** update missing values using TEC tables
use `eurostat_stock_flow2', clear
merge 1:1 iso2_o iso2_d year using `eurostat_stock_flow1', update nogen

merge 1:1 iso2_o iso2_d year using `eurostat_sales', nogen

**** standardize iso2 codes; convert millions of Euros into USD

foreach x in o d {
	ren iso2_`x' geo
	merge m:1 geo using `geo', keep(master match) nogen
	ren geo iso2_`x'
	ren geo_des countryName_`x'_eurostat
	
	estpost tabulate countryName_`x'_eurostat if length(iso2_`x')~=2
	esttab . using "`outputPath'", append varlabels(`e(labels)') nogaps noobs nonotes noabbrev ///
	title("Region names when iso2_`x' is not two-digit")
	estpost tabulate countryName_`x'_eurostat if length(iso2_`x')==2
	esttab . using "`outputPath'", append varlabels(`e(labels)') nogaps noobs nonotes noabbrev ///
	title("Region names when iso2_`x' is not two-digit")
	replace iso2_`x'="GB" if iso2_`x'=="UK" // UK
	replace iso2_`x'="GR" if iso2_`x'=="EL" // Greece
}

merge m:1 year using `euroExchRate', keep(master match) nogen keepusing(exchRate)
foreach x in out in {
	foreach y in stock flow sales {
		replace eurostat_`x'_`y' = eurostat_`x'_`y' * exchRate // Millions of EUR to Millions of USD
		}
	}
drop exchRate	

compress
save "ProcessedData/eurostat_FDI.dta", replace

*************************************
*** adjust outward sales for the financial sector
*************************************
file open myfile using "`outputPath'", write append
file write myfile _n _n "Adjust outward sales for the financial sector" _n

use "ProcessedData/nonfin_output_share.dta", clear
egen maxYr = max(year), by(iso3)
keep if year==maxYr
keep iso3 nonfin_output_share
tempfile lastYear_nonfin
save `lastYear_nonfin', replace

use `lastYear_nonfin', clear
gen year = 2012
tempfile nonfin_2012
save `nonfin_2012', replace

use "ProcessedData/nonfin_output_share.dta", clear
keep if year>=2007
append using `nonfin_2012'
fillin iso3 year
merge m:1 iso3 using `lastYear_nonfin', keep(1 3 4 5) update nogen
keep iso3 year nonfin_output_share
tempfile nonfin_output_share_0712
save `nonfin_output_share_0712', replace

use "ProcessedData/eurostat_FDI.dta", clear

*** outward sales for all years ***
preserve
keep if iso2_d=="WORLD"
keep eurostat_out_fin_sales eurostat_out_sales iso2_o year
gen world_out_nonfin_share_temp = 1 - eurostat_out_fin_sales/eurostat_out_sales
*** three year average
fillin iso2_o year
encode iso2_o, gen(id_iso2_o)
xtset id_iso2_o year
gen world_out_nonfin_share_1 = l.world_out_nonfin_share_temp
gen world_out_nonfin_share_3 = f.world_out_nonfin_share_temp
egen world_out_nonfin_share = rowmean(world_out_nonfin_share_1 world_out_nonfin_share_3 world_out_nonfin_share_temp)
keep iso2_o year world_out_nonfin_share
tempfile world_out_nonfin_share
save `world_out_nonfin_share', replace
restore

merge m:1 iso2_o year using `world_out_nonfin_share', keep(master match) nogen
ren iso2_o iso2
merge m:1 iso2 using "ProcessedData/isoStandard.dta", keep(master match) keepusing(iso3) nogen
merge m:1 iso3 year using "ProcessedData/nonfin_output_share.dta", keep(master match) ///
	keepusing(nonfin_output_share) nogen

gen eurostat_out_sales_nonfin = eurostat_out_sales
gen need_adjust = eurostat_out_sales~=. & eurostat_out_sales~=0
count if need_adjust==1
file write myfile "Number of outward sales that need to be adjusted: " (r(N)) _n

count if need_adjust==1 & ~missing(eurostat_out_fin_sales)
file write myfile "Step 1: number of obs adjusted using bilateral FIN sales: " (r(N)) _n
replace eurostat_out_sales_nonfin = eurostat_out_sales_nonfin - eurostat_out_fin_sales if need_adjust==1 & ~missing(eurostat_out_fin_sales)
replace need_adjust=0 if need_adjust==1 & ~missing(eurostat_out_fin_sales)

count if need_adjust==1 & ~missing(world_out_nonfin_share)
file write myfile "Step 2: number of obs adjusted using total outward World MP sales in financial sectors: " (r(N)) _n
replace eurostat_out_sales_nonfin = eurostat_out_sales_nonfin * world_out_nonfin_share if need_adjust==1 & ~missing(world_out_nonfin_share)
replace need_adjust=0 if need_adjust==1 & ~missing(world_out_nonfin_share)

count if need_adjust==1 & ~missing(nonfin_output_share)
file write myfile "Step 3: number of obs adjusted using share of nonfinancial output in the host country: " (r(N)) _n
replace eurostat_out_sales_nonfin = eurostat_out_sales_nonfin * nonfin_output_share if need_adjust==1 & ~missing(nonfin_output_share)
replace need_adjust=0 if need_adjust==1 & ~missing(nonfin_output_share)

merge m:1 iso3 year using `nonfin_output_share_0712', update keep(1 3 4 5) nogen
count if need_adjust==1 & ~missing(nonfin_output_share)
file write myfile "Step 4: number of obs adjusted using share of nonfinancial output in the host country 2010-2012, which are likely using the last year of data available before 2009: " (r(N)) _n
replace eurostat_out_sales_nonfin = eurostat_out_sales_nonfin * nonfin_output_share if need_adjust==1 & ~missing(nonfin_output_share)
replace need_adjust=0 if need_adjust==1 & ~missing(nonfin_output_share)

file close myfile
estpost tabulate year iso2 if need_adjust==1, missing
esttab . using "`outputPath'", append varlabels(`e(labels)') cell(b) ///
	unstack nonumber nogaps noobs nonotes noabbrev ///
	title("Countries and years that cannot be adjusted using the above 4 steps. Set to missing.")	
replace eurostat_out_sales = . if need_adjust==1

drop nonfin_output_share need_adjust world_out_nonfin_share eurostat_out_fin_sales eurostat_out_fin_sales_flag
ren iso2 iso2_o

gen share_out_sales = eurostat_out_sales_nonfin / eurostat_out_sales
estpost sum share_out_sales, detail
esttab . using "`outputPath'", append cells("mean sd count min p1 p10 p25 p50 p75 p90 p99 max") noobs ///
		title("Summary stats for nonfinancial outward sales in total sales")
drop share_out_sales
ren  eurostat_in_sales eurostat_in_sales_nonfin

drop iso3
compress
save "${PATH_OUT_DATA}/eurostat/eurostat_FDI_raw.dta", replace

** remove temporary files
!rmdir "${PATH_OUT_DATA}/Temp" /s /q // to delete nonempty folders need to use shell commands


