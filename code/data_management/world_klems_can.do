**************************************************
** import Canadian data from World KLEMS
**
** input: source_data/worldKlems/extraCountries/can_output_08I.xls
**
**************************************************

*** import Taiwan data
*** Gross Output
foreach z in GO VA {
	import excel using "source_data/worldKlems/extraCountries/can_output_08I.xls", sheet("`z'") cellrange(b1:ak32) firstrow clear
	gen codeID = _n
	** list code codeID // check industry temporary ID
	drop code
	reshape long _, i(codeID) j(year)
	ren _ value
	reshape wide value, i(year) j(codeID)
	egen tot_`z' = rowtotal(value*)
	egen manu`z' = rowtotal(value3-value15)
	gen nonfin_total`z' = tot_`z' - value24
	keep nonfin_total`z' manu`z' tot_`z' year
	tempfile can`z'
	save `can`z''
}

use `canGO', clear
merge 1:1 year using `canVA', nogen
gen iso3 = "CAN"

