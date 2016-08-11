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

**************************************************
*** combine the two data sets (outward FATS)
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

use "`data_in'/FATS_OUT3_bilat_tot_fin.dta", clear
ren *_ind6895 *_fin_isic3
ren *_ind9999 *_tot_isic3
tempfile isic3_out
save `isic3_out', replace

use "`data_in'/AMNE_OUT4_bilat_tot.dta", clear
merge 1:1 iso3_o iso3_d year using `isic3_out', nogen
foreach x in n_emp n_ent n_psn_emp rev {
    gen `x'_tot = `x'_C9999 if year>=2008
    if "`x'" ~= "n_psn_emp" {
        replace `x'_tot = `x'_tot_isic3 if year>=2008 & `x'_tot==.
        replace `x'_tot = `x'_tot_isic3 if year<=2007
    }
    replace `x'_tot = `x'_C9999 if year <= 2007 & `x'_tot==.
    
    gen flag_`x'_tot = flag_`x'_C9999 if year>=2008
    if "`x'" ~= "n_psn_emp" {
        replace flag_`x'_tot = flag_`x'_tot_isic3 if year>=2008 & flag_`x'_tot==""
        replace flag_`x'_tot = flag_`x'_tot_isic3 if year<=2007
    }
    replace flag_`x'_tot = flag_`x'_C9999 if year <= 2007 & flag_`x'_tot==""
}
ren *_fin_isic3 *_fin // only ISIC3 contains data on financial sector
drop *C9999 *isic3

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
*** before 2007 use isic3 as primary
*** after 2007 use isic4 as primary
**************************************************
use "`data_in'/FATS_IN3_bilat_tot_fin.dta", clear
merge 1:1 iso3_o iso3_d year using "`data_in'/AMNE_IN4_bilat_tot.dta", nogen

foreach x in n_emp n_ent rev {
    gen `x'_tot = `x'_ind9999 if year<=2007
    replace `x'_tot = `x'_C9999 if year<=2007 & `x'_tot==.
    replace `x'_tot = `x'_C9999 if year>=2008
    replace `x'_tot = `x'_ind9999 if year>=2008 & `x'_tot==.
    gen flag_`x'_tot = flag_`x'_ind9999 if year<=2007
    replace flag_`x'_tot = flag_`x'_C9999 if year<=2007 & flag_`x'_tot==""
    replace flag_`x'_tot = flag_`x'_C9999 if year>=2008
    replace flag_`x'_tot = flag_`x'_ind9999 if year>=2008 & flag_`x'_tot==""

    gen `x'_totXfin = `x'_ind9994 if year<=2007
    replace `x'_totXfin = `x'_C9994 if year<=2007 & `x'_tot==.
    replace `x'_totXfin = `x'_C9994 if year>=2008
    replace `x'_totXfin = `x'_ind9994 if year>=2008 & `x'_tot==.
    gen flag_`x'_totXfin = flag_`x'_ind9994 if year<=2007
    replace flag_`x'_totXfin = flag_`x'_C9994 if year<=2007 & flag_`x'_tot==""
    replace flag_`x'_totXfin = flag_`x'_C9994 if year>=2008
    replace flag_`x'_totXfin = flag_`x'_ind9994 if year>=2008 & flag_`x'_tot==""
}    

ren *_ind6895 *_fin
ren *n_psn_emp_C9994 *n_psn_emp_totXfin
ren *n_psn_emp_C9999 *n_psn_emp_tot

drop *_ind* *_C*

*** check growth rates
egen id_pair = group(iso3_o iso3_d)
xtset id_pair year
ds *n_psn_emp* *n_ent* *_n_emp* *rev*, has(type numeric)
foreach x in `r(varlist)' {
    gen diff_log_`x' = log(`x') - log(l.`x')
}
estpost tabstat diff_log_*, by(year) statistics(mean count) ///
    columns(statistics) quietly
esttab . using "`outputPath'", append main(mean) aux(count) nogap nostar ///
    unstack noobs nonote label ///
    title("diff log points growth - check break")    
drop diff_log_* id_pair
    
sort iso3_o iso3_d year
compress
save "`data_out'/activity_in.dta", replace
    
log close _all
