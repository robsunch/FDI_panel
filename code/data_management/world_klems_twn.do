**************************************************
** import Taiwan data from World KLEMS
**
** input: source_data/worldKlems/extraCountries/TAIWAN-Basic File_2013.xlsx
**
**************************************************

*** import Taiwan data
*** Gross Output
import excel using "source_data/worldKlems/extraCountries/TAIWAN-Basic File_2013.xlsx", sheet("GO") clear
foreach x of varlist D-AG {
	local yr = `x'[1]
	ren `x' _`yr'
	}
drop in 1
destring C, replace
drop A B
*** manufacturing 3-15, finance 24
reshape long _, i(C) j(year)
ren _ value
reshape wide value, i(year) j(C)
egen tot_GO = rowtotal(value*)
egen manuGO = rowtotal(value3-value15)
gen nonfin_totalGO = tot_GO - value24
keep nonfin_totalGO manuGO tot_GO year
tempfile twnGO
save `twnGO'

import excel using "source_data/worldKlems/extraCountries/TAIWAN-Basic File_2013.xlsx", sheet("VA") clear
foreach x of varlist C-AF {
	local yr = `x'[1]
	ren `x' _`yr'
	}
drop in 1
destring B, replace
drop A
*** manufacturing 3-15, finance 24
reshape long _, i(B) j(year)
ren _ value
reshape wide value, i(year) j(B)
egen tot_VA = rowtotal(value*)
egen manuVA = rowtotal(value3-value15)
gen nonfin_totalVA = tot_VA - value24

keep nonfin_totalVA manuVA tot_VA year

merge 1:1 year using `twnGO', nogen
gen iso3 = "TWN"
