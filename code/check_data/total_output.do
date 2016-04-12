*********************************************
** this do-file checks consistency of total output across
** different datasets (OECD-STAN and World KLEMS and 
** data provided by RRT)
*********************************************

local outputPath = "$tableDir/total_output.csv"
capture rm "`outputPath'"

** country aggregate from RRT data
use "source_data/Ramondo-appendix-data/appendix-dataset.dta", clear
merge 1:1 ISO_d ISO_o using "source_data/Ramondo-appendix-data/bilateral_mp.dta", keepusing(MandA) keep(master match) nogen
foreach x in o d {
    replace ISO_`x' = "DEU" if ISO_`x'=="GER"
    replace ISO_`x' = "ROU" if ISO_`x'=="ROM"
    replace ISO_`x' = "SVN" if ISO_`x'=="SLV"
    ren ISO_`x' iso3_`x'
}   
keep iso3_d gdp_d gross_prod_nonfin_d
duplicates drop
ren (iso3_d gdp_d gross_prod_nonfin_d) (iso3 gdp_rrt nonfin_output_rrt)
tempfile rrt_output
save `rrt_output', replace

***** FATS table National Total *****
use if iso3_o == "WRT" using "processed_data/activity_OECD_eurostat_combined.dta", clear
drop iso3_o
ren iso3_d iso3
keep iso3 year es_in_rev_totXfin oecd_in_rev_totXfin oecd_in_rev_tot
tempfile fats_output
save `fats_output', replace

***** World Klems and OECD-Stan *****
use "processed_data/worldKlems_va_go.dta", clear
merge 1:1 iso3 year using "processed_data/stan_prod.dta", nogen

foreach x in nonfin_totalGO tot_GO {
    gen diff_`x' = `x'_Klems / `x'_stan - 1
}

estpost sum diff_*, detail quietly
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("difference between Klems and STAN data")
estpost tabulate year iso3 if abs(diff_nonfin_totalGO)>0.05 & diff_nonfin_totalGO < .
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Number of cases for where diff in nonfin output > 0.05 or < -0.05") ///
            eqlabels(, lhs("year \ iso3_o"))
drop diff_*

*** check differences between total output from FATS tables
*** and those from STAN-KLEMS
merge 1:1 iso3 year using `fats_output', nogen
gen go_stan_klems = tot_GO_stan
replace go_stan_klems = tot_GO_Klems if go_stan_klems==.

gen nonfin_go_stan_klems = nonfin_totalGO_stan
replace nonfin_go_stan_klems = nonfin_totalGO_Klems if nonfin_go_stan_klems==.

gen diff_nonfin_oecd = oecd_in_rev_totXfin / nonfin_go_stan_klems - 1
gen diff_nonfin_es = es_in_rev_totXfin / nonfin_go_stan_klems - 1
gen diff_tot_oecd = oecd_in_rev_tot / go_stan_klems - 1
estpost sum diff_*, detail
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("difference in gross output between STAN-Klems and FATS tables (national total)")
drop diff_*

*** average between 1996-2001, against RRT data
keep if year>=1996 & year<=2001
collapse (mean) nonfin_totalGO_Klems nonfin_totalGO_stan ///
    (count) nonmiss_Klems = nonfin_totalGO_Klems ///
            nonmiss_stan = nonfin_totalGO_stan, by(iso3)
foreach x in Klems stan {
    replace nonfin_totalGO_`x' = . if nonmiss_`x' < 6 // no average if any year is missing
}
merge 1:1 iso3 using `rrt_output', nogen

gen diff_klems = nonfin_totalGO_Klems / nonfin_output_rrt - 1
gen diff_stan = nonfin_totalGO_stan / nonfin_output_rrt - 1

estpost sum diff_*, detail
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("difference between Klems/STAN 1996-2001 average and the RRT data")            

