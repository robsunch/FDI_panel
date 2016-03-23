*****************************************************
** this do-file imports Eurostat bilateral affiliate sales, 
** FDI flows and FDI stocks data
** transform them into dta files
*****************************************************

log close _all
log using "$logDir/eurostat_to_dta.smcl", replace

** temporary directory structure
capture mkdir "processed_data/temp"
capture mkdir "processed_data/temp/eurostat"
capture mkdir "processed_data/temp/eurostat/fats_96"

**************
** FATS tables
**************
*** fats_96
local fileList : dir "source_data/eurostat/fats_96/" files "fats_*.tsv"
foreach f in `fileList' {
	insheet using "source_data/eurostat/fats_96/`f'", tab clear
	local fileName = subinstr("`f'",".tsv",".dta",.)
	
	if "`f'" ~= "fats_de.tsv" {
		quietly ds v1, not
		local yearList = r(varlist)
		split v1, parse(",") gen(v1_)
		drop v1
		foreach x in `yearList' {
			local yr = `x'[1]
			ren `x' value`yr'
			}
		foreach x of varlist v1_1 - v1_3 {
			local xName = `x'[1]
			ren `x' `xName'
			}
		ren v1_4 geo
		drop in 1
	}
	else {
		quietly ds v1, not
		local ctyList = r(varlist)
		split v1, parse(",") gen(v1_)
		drop v1
		foreach x in `ctyList' {
			local cty = `x'[1]
			ren `x' value`cty'
			}
		
		foreach x of varlist v1_1 - v1_3 {
			local xName = `x'[1]
			ren `x' `xName'
			}
		ren v1_4 year
		drop in 1
		reshape long value, i(nace_r1 indic_sb geo year) j(c_ctrl) string
		reshape wide value, i(nace_r1 indic_sb geo c_ctrl) j(year) string
	}

	save "processed_data/temp/eurostat/fats_96/`fileName'", replace
}

local fileList : dir "source_data/eurostat/" files "fats_*.tsv"
foreach f in `fileList' {
	local fileName = subinstr("`f'",".tsv",".dta",.)
	insheet using "source_data/eurostat/`f'", tab clear
	quietly ds v1, not
	local yearList = r(varlist)
	split v1, parse(",") gen(v1_)
	drop v1
	foreach x in `yearList' {
		local yr = `x'[1]
		ren `x' value`yr'
		}
	foreach x of varlist v1_1 - v1_3 {
		local xName = `x'[1]
		ren `x' `xName'
		}
	ren v1_4 geo
	drop in 1
	save "processed_data/temp/eurostat/`fileName'", replace
}

**********************************
** eurostat FDI flows and stocks with ROW
**********************************
** tec tables
local fileList : dir "source_data/eurostat/" files "tec*.tsv"
foreach f in `fileList' {
	local fileName = subinstr("`f'",".tsv",".dta",.)
	insheet using "source_data/eurostat/`f'", tab clear
	quietly ds v1, not
	local yearList = r(varlist)
	split v1, parse(",") gen(v1_)
	drop v1
	foreach x in `yearList' {
		local yr = `x'[1]
		ren `x' value`yr'
		}
	foreach x of varlist v1_1 - v1_3 {
		local xName = `x'[1]
		ren `x' `xName'
		}
	ren v1_4 geo
	drop in 1
	duplicates report
	save "processed_data/temp/eurostat/`fileName'", replace
}

** bop_fdi tables
local fileList : dir "source_data/eurostat/" files "bop_fdi_*.tsv"
foreach f in `fileList' {
	local fileName = subinstr("`f'",".tsv",".dta",.)
	insheet using "source_data/eurostat/`f'", tab clear
	quietly ds v1, not
	local yearList = r(varlist)
	split v1, parse(",") gen(v1_)
	drop v1
	foreach x in `yearList' {
		local yr = `x'[1]
		ren `x' value`yr'
		}
	if regexm("`f'","pos") {		
		foreach x of varlist v1_1 - v1_4 {
			local xName = `x'[1]
			ren `x' `xName'
			}
		ren v1_5 geo
		}
	else {
		foreach x of varlist v1_1 - v1_5 {
			local xName = `x'[1]
			ren `x' `xName'
			}
		ren v1_6 geo
		}
			
	drop in 1
	duplicates report
	save "processed_data/temp/eurostat/`fileName'", replace
}

log close _all
