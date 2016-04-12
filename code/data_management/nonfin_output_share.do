**********************************************************
** this do-file calculates the share of non-financial sector
** in total output in all countries with data available
**
** input:   processed_data/stan_prod.dta
**      .../worldKlems_va_go.dta
**
** output:  processed_data/nonfin_output_share.dta
**********************************************************

use "processed_data/stan_prod.dta", clear
merge 1:1 iso3 year using "processed_data/worldKlems_va_go.dta", nogen

foreach x in stan Klems {
    gen nonfin_output_share_`x' = nonfin_totalGO_`x' / tot_GO_`x'
    }
    
gen nonfin_output = nonfin_totalGO_stan if nonfin_output_share_stan < .
gen output_source = "STAN"
gen nonfin_output_share_temp = nonfin_output_share_stan
replace nonfin_output = nonfin_totalGO_Klems if nonfin_output_share_temp == . ///
    & nonfin_output_share_Klems < .
replace output_source = "Klems" if nonfin_output_share_temp == . ///
    & nonfin_output_share_Klems < .
replace nonfin_output_share_temp = nonfin_output_share_Klems if missing(nonfin_output_share_temp)

encode iso3, gen(id_iso3)
xtset id_iso3 year
gen nonfin_output_share_1 = l.nonfin_output_share_temp
gen nonfin_output_share_3 = f.nonfin_output_share_temp

egen nonfin_output_share = rowmean(nonfin_output_share_1 nonfin_output_share_3 nonfin_output_share_temp)
keep iso3 year nonfin_output_share nonfin_output output_source
compress
save "processed_data/nonfin_output_share.dta", replace
