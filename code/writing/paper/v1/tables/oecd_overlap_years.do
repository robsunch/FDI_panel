******************************************
** compare coverage and values of OECD ISIC3 and ISIC4 
******************************************

local data_in = "processed_data/OECD"
local data_out = "processed_data/OECD"

label define _merge_lab 1 "ISIC4 only" 2 "ISIC3 only" 3 "Both"

use using "`data_in'/AMNE_OUT4_bilat_tot.dta", clear
merge 1:1 iso3_o iso3_d year using "`data_in'/FATS_OUT3_bilat_tot_fin.dta"
label drop _merge
label values _merge _merge_lab
keep if year>=2006 & year<=2010

estpost tabulate year _merge
esttab using "$tableDir/oecd_overlap_years_coverage_out.tex", replace cell(b) unstack ///
    noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
    title("Number of observations (origin*destination) from each outward MP dataset") booktabs

use using "`data_in'/AMNE_IN4_bilat_tot.dta", clear
merge 1:1 iso3_o iso3_d year using "`data_in'/FATS_IN3_bilat_tot_fin.dta"

keep if year>=2007 & year<=2009
label drop _merge
label values _merge _merge_lab
estpost tabulate year _merge
esttab using "$tableDir/oecd_overlap_years_coverage_in.tex", replace cell(b) unstack ///
    noobs nonumber nomtitle nodepvars nostar varlabels(`e(labels)') collabels(none) ///
    title("Number of observations (origin*destination) from each outward MP dataset") booktabs
    
****** summarize differences *******
use using "`data_in'/AMNE_OUT4_bilat_tot.dta", clear
merge 1:1 iso3_o iso3_d year using "`data_in'/FATS_OUT3_bilat_tot_fin.dta"
foreach x in n_ent n_emp rev {
    gen diff_log_`x' = log(`x'_ind9999) - log(`x'_C9999)
}
estpost summarize diff*, detail quietly
local prehead_str "\begin{table}[h]\scriptsize\caption{Diff total outward isic3 v.s. isic4}\centering" ///
    "\begin{threeparttable}\begin{tabular}{l*{10}c}\toprule"
local postfoot_str "\bottomrule\end{tabular}\begin{tablenotes}" ///
    "\item[a] Diff in log points (isic3 - isic4)." ///
    "\end{tablenotes}\end{threeparttable}\end{table}"
esttab . using "$tableDir/oecd_overlap_years_diff_out.tex", replace ///
    noobs nonumber nomtitle nodepvars nostar booktabs ///
    cell("count(fmt(a3)) mean sd min p10 p25 p50 p75 p90 max") ///
    prehead("`prehead_str'") postfoot("`postfoot_str'")
    
use using "`data_in'/AMNE_IN4_bilat_tot.dta", clear
merge 1:1 iso3_o iso3_d year using "`data_in'/FATS_IN3_bilat_tot_fin.dta"
foreach x in n_ent n_emp rev {
    gen diff_log_`x' = log(`x'_ind9994) - log(`x'_C9994)
}
estpost summarize diff*, detail quietly
local prehead_str "\begin{table}[h]\scriptsize\caption{Diff nonfin inward isic3 v.s. isic4}\centering" ///
    "\begin{threeparttable}\begin{tabular}{l*{10}c}\toprule"
local postfoot_str "\bottomrule\end{tabular}\begin{tablenotes}" ///
    "\item[a] Diff in log points isic3 - isic4." ///
    "\end{tablenotes}\end{threeparttable}\end{table}"
esttab . using "$tableDir/oecd_overlap_years_diff_in.tex", replace ///
    noobs nonumber nomtitle nodepvars nostar booktabs ///
    cell("count(fmt(a3)) mean sd min p10 p25 p50 p75 p90 max") ///
    prehead("`prehead_str'") postfoot("`postfoot_str'")
