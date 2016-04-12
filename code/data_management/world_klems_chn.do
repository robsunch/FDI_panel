**************************************************
** import Chinese data from World KLEMS
**
** input: source_data/worldKlems/extraCountries/CIP_Round_1.0_(Dec_2011).xls
**
**************************************************

*** import Chinese data
foreach z in GVO GVA1 {
	*** total nonfinancial industry
	if "`z'"=="GVO" {
		local newz = "GO"
		}
	else {
		local newz = "VA"
		}
		
	import excel using "source_data/worldKlems/extraCountries/CIP_Round_1.0_(Dec_2011).xls", sheet("`z'") clear
	foreach x of varlist C-X {
		local yr = `x'[1]
		ren `x' _`yr'
		}
	keep if inlist(A,"TT","J")
	drop B
	gen iso3 = "CHN"
	reshape long _, i(iso3 A) j(year)
	destring _, replace
	ren _ value
	reshape wide value, i(iso3 year) j(A) string
	gen nonfin_total`newz' = valueTT - valueJ
	ren valueTT tot_`newz'
	drop valueJ
	
	tempfile chn_nonfin_total_`z'
	save `chn_nonfin_total_`z''

	*** aggregate manufacturing
	import excel using "source_data/worldKlems/extraCountries/CIP_Round_1.0_(Dec_2011).xls", sheet("`z'") clear
	foreach x of varlist C-X {
		local yr = `x'[1]
		ren `x' _`yr'
		}
	split A, parse("t")
	** a careful check shows that sectors that have ISIC 2 digit codes between 15 and 37
	** are mutually exclusive and completely span the manufacturing sector
	destring A1 A2, gen(num_A1 num_A2) force
	keep if ( num_A1>=15 & num_A2<=37 & ~missing(num_A1)) | (num_A1>=15 & num_A1<=37)
	drop B
	gen iso3 = "CHN"
	reshape long _, i(iso3 A) j(year)
	destring _, replace
	ren _ manu`newz'
	collapse (sum) manu`newz', by(iso3 year)

	keep manu`newz' iso3 year
	tempfile chn_manu_`z'
	save `chn_manu_`z''

}

foreach x in manu nonfin_total {
	foreach z in GVO GVA1 {
		merge 1:1 iso3 year using `chn_`x'_`z'', nogen
		}
	}

