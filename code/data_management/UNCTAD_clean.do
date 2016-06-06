***************************************************
** additional data cleaning
** attach country code
** drop duplicates when numbers are the same
** adjust cases where the same entry
** in the two countries' tables are different (same reporting country)
****************************************************
log close _all
log using "$logDir/UNCTAD_clean.smcl", replace

local outputPath = "$tableDir/UNCTAD_clean.txt"
file close _all
file open myfile using "`outputPath'", write replace
file write myfile "This file records information for the cleaning of the UNCTAD bilateral FDI data." _n

import excel using "processed_data/isoStandard.xlsx", sheet("UNCTAD_output_match_iso3") clear firstrow
ren iso3_standard iso3
keep iso3 iso3_UNCTAD
drop if missing(iso3) | missing(iso3_UNCTAD)
duplicates drop
tempfile iso3_UNCTAD_output_match_iso3
save `iso3_UNCTAD_output_match_iso3', replace

import excel using "processed_data/isoStandard.xlsx", sheet("UNCTAD_output_match_iso3") clear firstrow
ren iso3_standard iso3
keep iso3 countryName_UNCTAD
drop if missing(iso3) | missing(countryName_UNCTAD)
duplicates drop
tempfile name_UNCTAD_output_match_iso3
save `name_UNCTAD_output_match_iso3', replace

import excel using "processed_data/isoStandard.xlsx", sheet("UNCTAD_output_no_match_iso3") clear firstrow
keep countryName iso3
ren countryName countryName_UNCTAD
drop if missing(iso3)
tempfile UNCTAD_output_no_match_iso3
save `UNCTAD_output_no_match_iso3', replace

use "processed_data/UNCTAD_raw.dta", clear
foreach x in o d {
    ren (iso3_`x' ctyName_`x') (iso3_UNCTAD countryName_UNCTAD)
    merge m:1 iso3_UNCTAD using `iso3_UNCTAD_output_match_iso3', keep(master match) nogen
    merge m:1 countryName_UNCTAD using `name_UNCTAD_output_match_iso3', ///
        update keep(1 3 4 5) nogen
    merge m:1 countryName_UNCTAD using `UNCTAD_output_no_match_iso3', ///
        update keep(1 3 4 5) nogen
    drop if missing(iso3)
    drop countryName_UNCTAD iso3_UNCTAD
    ren iso3 iso3_`x'
}

** map reporting country names to iso3
ren reportCtyName countryName_UNCTAD
merge m:1 countryName_UNCTAD using `name_UNCTAD_output_match_iso3', ///
        update keep(1 3 4 5) nogen
merge m:1 countryName_UNCTAD using `UNCTAD_output_no_match_iso3', ///
    update keep(1 3 4 5) nogen
drop if missing(iso3)  
drop countryName_UNCTAD
ren iso3 iso3_report

*** deal with duplicates of (iso3_o iso3_d iso3_report year)
*** because origin and destination countries may both record the origin's (or destination's) reported number
preserve
duplicates tag iso3_o iso3_d iso3_report year, gen(dup_tag)
keep if dup_tag==1
bysort iso3_o iso3_d iso3_report year: gen within_id = _n
reshape wide stock* flow*, i(iso3_o iso3_d iso3_report year) j(within_id)

file close myfile
foreach x in stock flow {
    count
    file open myfile using "`outputPath'", write append
    file write myfile _n "Number of cases where the `x' from the two countries' tables are both in the data : " (r(N)) _n
    file write myfile "Among which, " _n
    count if `x'1==`x'2
    file write myfile "     (1) the two values are exactly the same : " (r(N)) _n
    count if (~missing(`x'1) & missing(`x'2)) | (missing(`x'1) & ~missing(`x'2))
    file write myfile "     (2) one is missing, the other is not : " (r(N)) _n
    count if (~missing(`x'1) & `x'1~=0 & `x'2==0) | (~missing(`x'2) & `x'2~=0 & `x'1==0)
    file write myfile "     (3) one is zero, the other is not : " (r(N)) _n
    count if ~missing(`x'1) & `x'1~=0 & ~missing(`x'2) & `x'2~=0 & `x'1~=`x'2
    file write myfile "     (4) both are nonzero : " (r(N)) _n
    
    file close myfile

    gen abs_diff_`x' = abs(`x'2-`x'1) / (`x'1+`x'2) if ~missing(`x'1) & `x'1~=0 & ~missing(`x'2) & `x'2~=0 & `x'1~=`x'2
    estpost sum abs_diff_`x' 
    esttab . using "`outputPath'", append cells("mean sd count min max") noobs ///
        title("Summary stats for abs difference of `x' from the two tables when they are not exactly the same (and neither is zero or missing)")
    
    *** implement imputation
    gen `x' = `x'1 if `x'1==`x'2
    replace `x' = `x'1 if (~missing(`x'1) & missing(`x'2)) | (~missing(`x'1) & `x'1~=0 & `x'2==0)
    replace `x' = `x'2 if (missing(`x'1) & ~missing(`x'2)) | (~missing(`x'2) & `x'2~=0 & `x'1==0)
    replace `x' = (`x'1 + `x'2) / 2 if ~missing(`x'1) & `x'1~=0 & ~missing(`x'2) & `x'2~=0 & `x'1~=`x'2 & abs_diff_`x'<0.1
    
    foreach y in notes source {
        gen `x'_`y' = `x'_`y'1 if `x'==`x'1
        replace `x'_`y' = `x'_`y'2 if `x'==`x'2 & missing(`x'_`y')
        replace `x'_`y' = "average" if ~missing(`x'1) & `x'1~=0 & ~missing(`x'2) & `x'2~=0 & `x'1~=`x'2 & abs_diff_`x'<0.1
        drop `x'_`y'1 `x'_`y'2
        }
    drop `x'1 `x'2
}

drop abs_diff* dup_tag
tempfile dup
save `dup', replace
restore

** cases where home and host countries are the same
estpost tab iso3_o if iso3_o==iso3_d, sort
esttab . using "`outputPath'", append cells("b") unstack noobs nolabel nonumber nomtitle ///
        title("Number of records where origin and destination countries are the same. Dropped.") ///
        varlabels(`e(labels)')
drop if iso3_o==iso3_d      

** drop cases where one flow is in both tables and supplement with the cleaned version
duplicates tag iso3_o iso3_d iso3_report year, gen(dup_tag)
keep if dup_tag==0
drop dup_tag
append using `dup'

*** now treat the values reported by origin as outflow
*** values reported by host as inflow
preserve
keep if iso3_d==iso3_report
ren (stock* flow*) (UNCTAD_in_stock* UNCTAD_in_flow*)
drop iso3_report
tempfile inwardFDI
save `inwardFDI', replace
restore

drop if iso3_o==iso3_report
drop iso3_report
ren (stock* flow*) (UNCTAD_out_stock* UNCTAD_out_flow*)
merge 1:1 iso3_o iso3_d year using `inwardFDI', nogen

compress
save "processed_data/UNCTAD_FDI.dta", replace
