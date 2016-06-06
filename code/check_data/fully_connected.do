*************************************************
** this do file checks the connectedness of MP in_rev_0611
** between countries
*************************************************

local outputPath = "$tableDir/fully_connected.txt"
capture rm "`outputPath'"
file close _all
file open myfile using "`outputPath'", write replace
file write myfile "This file records related information when construct the connected sample of countries by nonmissing MP." _n

*** we always start from the Tintelnot sample and search for additional countries
local z = "Tintelnot"
import excel countryName iso2 iso3 id in_arrySample in_TintelnotSample using "source_data/misc/ARRY_sample.xlsx", ///
    clear cellrange(a2:f250)
keep if in_TintelnotSample==1
replace in_TintelnotSample = 1 if missing(in_TintelnotSample)
drop if inlist(iso3, "CAN","CHE")
count
local numCty_Tintelnot = r(N)
file write myfile "We start from the " (r(N)) " countries in Tintelnot" _n
levelsof countryName
file write myfile "They are " (r(levels)) _n
levelsof iso3
file write myfile "Corresponding iso3 codes are " (r(levels)) _n
keep iso3 in_TintelnotSample
tempfile TintelnotSample
save `TintelnotSample'

** sample countries
use "processed_data/extrap_bilat_activities.dta", clear

foreach x in o d {
    ren iso3_`x' iso3
    merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", ///
        keep(match) keepusing(iso3) nogen
    drop if inlist(iso3,"MLT")
    ren iso3 iso3_`x'
}        

keep if year>=2006 & year<=2011
collapse (mean) in_rev_0611 = in_rev_step6 (count) num_nonmiss_0611 = in_rev_step6, by(iso3_o iso3_d)
keep if num_nonmiss_0611 >= 3

fillin iso3_o iso3_d
drop if iso3_o == iso3_d

tempfile bilateral_mp
save `bilateral_mp', replace


************************
*** extension step 1
************************
    use `bilateral_mp', clear
    foreach x in o d {
        ren iso3_`x' iso3
        merge m:1 iso3 using ``z'Sample', keep(master match) nogen // somehow Cyprus is in ARRY but not RRT
        replace in_`z'Sample = 0 if missing(in_`z'Sample)
        ren iso3 iso3_`x'
        ren in_`z'Sample in_`z'Sample_`x'
        }

    ** are all countries in the sample connected?
    ** check whether outflows are missing
    preserve
    replace in_`z'Sample_d = 0 if missing(in_rev_0611)
    collapse (sum) in_`z'Sample_d (first) in_`z'Sample_o, by(iso3_o)
    quietly count if in_`z'Sample_o==1 & in_`z'Sample_d < `numCty_`z'' - 1
    if r(N) > 0 {
        display "Not all countries are connected, check."
        list if in_`z'Sample_o==1 & in_`z'Sample_d < `numCty_`z'' - 1
        error 1
        }
        
    keep if (in_`z'Sample_o==0 & in_`z'Sample_d == `numCty_`z'') | in_`z'Sample_o==1 // source countries having outflows to all sample countries
    keep iso3_o
    ren iso3_o iso3
    tempfile ext_o_1
    save `ext_o_1', replace
    restore

    ** check whether inflows are missing
    preserve
    replace in_`z'Sample_o = 0 if missing(in_rev_0611)
    collapse (sum) in_`z'Sample_o (first) in_`z'Sample_d, by(iso3_d)
    quietly count if in_`z'Sample_d==1 & in_`z'Sample_o < `numCty_`z'' - 1
    if r(N) > 0 {
        display "Not all countries are connected, check."
        list if in_`z'Sample_d==1 & in_`z'Sample_o < `numCty_`z'' - 1
        error 1
        }
        
    keep if (in_`z'Sample_d==0 & in_`z'Sample_o == `numCty_`z'') | in_`z'Sample_d == 1 // destination countries having inflows from all sample countries
    keep iso3_d
    ren iso3_d iso3
    tempfile ext_d_1 
    save `ext_d_1', replace
    restore

    use `ext_o_1', clear
    merge 1:1 iso3 using `ext_d_1', keep(match) nogen
    quietly count
    local numCty_poten1 = r(N)
    keep iso3
    gen in_poten1Sample = 1
    tempfile poten1Sample
    save `poten1Sample', replace
    compress
    save "processed_data/connected_oecd_es_1.dta",replace

****************************************
*** check whether we found additional countries
*** and try extension step 2
****************************************
local z1 = "poten1"

use `bilateral_mp', clear
foreach x in o d {
    ren iso3_`x' iso3
    merge m:1 iso3 using ``z1'Sample', keep(master match) nogen
    replace in_`z1'Sample = 0 if missing(in_`z1'Sample)
    ren iso3 iso3_`x'
    ren in_`z1'Sample in_`z1'Sample_`x'
}

** are all countries in the potential sample connected?
** check whether outflows are missing
preserve
replace in_`z1'Sample_d = 0 if missing(in_rev_0611)
collapse (sum) in_`z1'Sample_d (first) in_`z1'Sample_o, by(iso3_o)

quietly count if in_`z1'Sample_o==1 & in_`z1'Sample_d < `numCty_`z1'' - 1
if r(N) > 0 {
    display "Not all countries are connected, check outflows."
    list if in_`z1'Sample_o==1 & in_`z1'Sample_d < `numCty_`z1'' - 1
    ** error 1
}
keep if (in_`z1'Sample_o==0 & in_`z1'Sample_d == `numCty_`z1'') | in_`z1'Sample_o==1 // source countries having outflows to all sample countries
keep iso3_o
ren iso3_o iso3
tempfile ext_o_2
save `ext_o_2', replace
restore

** check whether inflows are missing
preserve
replace in_`z1'Sample_o = 0 if missing(in_rev_0611)
collapse (sum) in_`z1'Sample_o (first) in_`z1'Sample_d, by(iso3_d)
quietly count if in_`z1'Sample_d==1 & in_`z1'Sample_o < `numCty_`z1'' - 1
if r(N) > 0 {
    display "Not all countries are connected, check inflows."
    list if in_`z1'Sample_d==1 & in_`z1'Sample_o < `numCty_`z1'' - 1
    error 1
}
    
keep if (in_`z1'Sample_d==0 & in_`z1'Sample_o == `numCty_`z1'') | in_`z1'Sample_d == 1 // destination countries having inflows from all sample countries
keep iso3_d
ren iso3_d iso3
tempfile ext_d_2
save `ext_d_2', replace
restore

use `ext_o_2', clear
merge 1:1 iso3 using `ext_d_2', keep(match) nogen
quietly count
local numCty_poten2 = r(N)
if r(N) > `numCty_`z1'' {
    display "Number of countries increases in the second search attempt, check!"
    error 1
}



    