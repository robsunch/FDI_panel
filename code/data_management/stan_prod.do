******************************************
** import STAN output and value added data
**
** input: source_data/STAN/DATA.txt
**      source_data/PennWorldTable/8.0/pwt80.dta
**
** output:  processed_data/stan_prod.dta
******************************************


** import production data
insheet using "source_data/STAN/DATA.txt", delimiter("|") clear names case
keep if inlist(Var,"PROD","VALU")
compress
tempfile prod_valu
save `prod_valu', replace

use if ind=="0199" using `prod_valu', clear
keep Cou Var value year
ren value tot_
tempfile tot_
save `tot_'

use if ind=="6567" using `prod_valu', clear
keep Cou Var value year
ren value fin_
merge 1:1 Cou Var year using `tot_',keep(match) nogen
reshape wide fin_ tot_, i(Cou year) j(Var) string
ren Cou countrycode
merge 1:1 countrycode year using "source_data/PennWorldTable/8.0/pwt80.dta", keep(master match) keepusing(xr) nogen
ren countrycode iso3

gen nonfin_totalGO_stan = (tot_PROD - fin_PROD) / xr
gen tot_GO_stan = tot_PROD / xr
gen nonfin_totalVA_stan = (tot_VALU - fin_VALU) / xr
gen tot_VA_stan = tot_VALU / xr

label var nonfin_totalGO_stan "STAN nonfinancial gross output, current USD"
label var tot_GO_stan "STAN total gross output, current USD"
label var nonfin_totalVA_stan "STAN nonfinancial value added, current USD"
label var tot_VA_stan "STAN total value added, current USD"
keep iso3 year *_stan
compress

save "processed_data/stan_prod.dta", replace    
    