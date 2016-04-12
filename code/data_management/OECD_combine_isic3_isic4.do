****************************************************
** this do-file compares the common years for ISIC3 (FATS) and 
** ISIC4 (AMNE) in the OECD data
** pick one source for the common years (2007-2009)
****************************************************

log close _all
log using "$logDir/OECD_combine_isic3_isic4.smcl", replace

local data_in = "processed_data/OECD"
local data_out = "processed_data/OECD"
local outputPath = "$tableDir/OECD_combine_isic3_isic4.csv"
capture rm "`outputPath'"

*** EUR to USD exchange rate
use iso3 fixedRate if iso3=="SVN" using "processed_data/exchRate.dta", clear
quietly sum fixedRate
local fixedRate_SVN = `r(mean)' // for SVN

*** compare outward MP
use using "`data_in'/AMNE_OUT4_bilat_tot.dta", clear
merge 1:1 iso3_o iso3_d year using "`data_in'/FATS_OUT3_bilat_tot_fin.dta"
estpost tabulate year _merge
esttab using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
    title("Number of observations (origin * source * year) from each outward MP dataset") ///
    addnotes("_merge=1 if in ISIC4 data only and _merge=2 if in ISIC3 data only") ///
    eqlabels(, lhs("year \ merge"))

keep if year<=2009 & year>=2007

foreach x in n_emp n_ent rev {
    gen nonmiss_`x'_ind9999 = `x'_ind9999 < .
    gen nonmiss_`x'_C9999 = `x'_C9999 < .
    gen nonmiss_`x'_ind6895 = `x'_ind6895 < .
    
    forvalues yr = 2007(1)2009 {
        estpost tabulate nonmiss_`x'_ind9999 nonmiss_`x'_C9999 if year==`yr'
        esttab using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle  ///
            title("Number of nonmissing cases for `x' - outward MP - year `yr'") ///
            eqlabels(, lhs("isic3 \ isic4"))
        estpost tabulate nonmiss_`x'_ind9999 nonmiss_`x'_ind6895 if year==`yr'
        esttab using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Number of nonmissing cases for `x' - isic3 outward MP -year `yr'") ///
            eqlabels(, lhs("tot \ fin"))
    }
    
    gen diff_`x' = `x'_ind9999 / `x'_C9999 - 1    
}    

estpost summarize diff*, detail quietly
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("% diff (isic3 vs isic4)")

estpost tabulate iso3_o year if (diff_rev > 1 & diff_rev < .) | diff_rev < -0.5
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Number of cases for where % diff in revenue > 1 or < -0.5 before SVN exchange rate adjustment") ///
            eqlabels(, lhs("year \ iso3_o"))
foreach x in 6895 9999 {
    replace rev_ind`x' = rev_ind`x' / `fixedRate_SVN' if iso3_o == "SVN"
}
replace diff_rev = rev_ind9999 / rev_C9999 - 1
estpost summarize diff_rev, detail quietly
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("% diff (isic3 vs isic4) after adjusting for exchange rate for SVN")

**************************************************
*** combine the two data sets
*** before 2007 (including 2007) use isic3 ; after 2007 use isic4
**************************************************
use "`data_in'/AMNE_OUT4_world_tot_fin.dta", clear // note for outward world total/fin, data is in a separate table
ren (*C6466 *C9999) (*fin *tot)
replace iso3_d = "WORLD"
preserve
keep iso3* year *fin
tempfile AMNE_OUT4_world_fin
save `AMNE_OUT4_world_fin', replace
restore
keep iso3* year *tot
tempfile AMNE_OUT4_world_tot
save `AMNE_OUT4_world_tot', replace

use if year<=2007 using "`data_in'/FATS_OUT3_bilat_tot_fin.dta", clear
ren *_ind6895 *_fin
ren *_ind9999 *_tot
tempfile before07
save `before07', replace

use if year>=2008 using "`data_in'/AMNE_OUT4_bilat_tot.dta", clear
ren *_C9999 *_tot
append using `before07'
merge 1:1 iso3_o iso3_d year using `AMNE_OUT4_world_fin', update
estpost tabulate year _merge, elabels
esttab using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
    title("Merge update with AMNE_OUT4_world_fin") ///
    eqlabels(, lhs("year \ merge"))
drop _merge
merge 1:1 iso3_o iso3_d year using `AMNE_OUT4_world_tot', update
esttab using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
    title("Merge update with AMNE_OUT4_world_tot") ///
    eqlabels(, lhs("year \ merge"))
drop _merge

sort iso3_o iso3_d year
compress
save "`data_out'/activity_out.dta", replace

*** compare inward MP
use using "`data_in'/AMNE_IN4_bilat_tot.dta", clear
merge 1:1 iso3_o iso3_d year using "`data_in'/FATS_IN3_bilat_tot_fin.dta"
estpost tabulate year _merge
esttab using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
    title("Number of observations (origin * source * year) from each inward MP dataset") ///
    addnotes("_merge=1 if in ISIC4 data only and _merge=2 if in ISIC3 data only") ///
    eqlabels(, lhs("year \ merge"))

**************************************************
*** combine the two data sets
*** before 2007 (including 2007) use isic3 ; after 2007 use isic4
**************************************************
use if year<=2007 using "`data_in'/FATS_IN3_bilat_tot_fin.dta", clear
ren (*_ind9994 *_ind6895 *_ind9999) (*_totXfin *_fin *_tot)
tempfile before07
save `before07', replace

use if year>=2008 using "`data_in'/AMNE_IN4_bilat_tot.dta", clear
ren (*_C9999 *_C9994) (*_tot *_totXfin)
append using `before07'


sort iso3_o iso3_d year
compress
save "`data_out'/activity_in.dta", replace
    
log close _all
