/*
This do-file imports FDI statistics from 
UNCTAD

*/

include project_paths
log using "$logDir/import_UNCTAD.smcl", replace

clear all
local fileList : dir "source_data/FDI/UNCTAD/" files "webdiaeia2014d3_*.xls", respectcase
tempfile inflows outflows instock outstock
foreach f in `fileList' {
	local ctyCode = subinstr("`f'",".xls","",.)
	local ctyCode = subinstr("`ctyCode'","webdiaeia2014d3_","",.)
	
	foreach sheetName in inflows outflows instock outstock {
	
		import excel using "source_data/UNCTAD/webdiaeia2014d3_`ctyCode'.xls", sheet("`sheetName'") clear allstring
		foreach x of varlist * {
			replace `x' = trim(`x')
			}
		dropmiss, force // drop variables with all obs missing
		
		*** retrieve country name
		if "A[1]"==".." {
			disp "Cell A1 is not country name for `ctyCode', check."
			error 1
			}
		local ctyName = A[1]
		
		*** retrieve notes and source
		gen obs = _n
		gen notes = ""
		quietly sum obs if regexm(A,"^Note: ")
		if `r(N)' > 0 {
			replace notes = subinstr(A[`r(min)'],"Note: ", "", 1)
			}
		replace A = "" if regexm(A,"^Note: ")
		gen source = ""
		quietly sum obs if regexm(A,"^Source: ")
		if `r(N)' > 0 {
			replace source = subinstr(A[`r(min)'],"Source: ","",1)	
		}		
		replace A = "" if regexm(A,"^Source: ") // for some countries, source and notes are mistakenly written in a line with values
		
		drop obs
		quietly ds A notes source, not
		egen numnonmiss = rownonmiss(`r(varlist)'), strok
		drop if numnonmiss==0 // after source, notes and country name is retrieved, these lines contain no more useful info
		drop numnonmiss
		
		if A[1]=="Reporting economy" {

			*** retrieve year, in line 1
			foreach x of varlist B-M {
				local yr = `x'[1]
				if "`sheetName'"=="inflows" | "`sheetName'"=="outflows" {
					ren `x' flow`yr'
					}
				else {
					ren `x' stock`yr'
					}
				}	
				
			drop if A=="Reporting economy"
			ren A reportCtyName
			
			count if reportCtyName==".."
			if `r(N)' > 0 {
				disp "Some reporting country is .. for `ctyCode', check."
				error 1
				}		
			
			if "`sheetName'"=="inflows" | "`sheetName'"=="instock" {
				gen ctyName_o = reportCtyName
				gen ctyName_d = "`ctyName'"
				gen iso3_d = "`ctyCode'"
			}
			else {
				gen ctyName_o = "`ctyName'"
				gen ctyName_d = reportCtyName
				gen iso3_o = "`ctyCode'"
			}

			if "`sheetName'"=="inflows" | "`sheetName'"=="outflows" {
				reshape long flow, i(reportCtyName) j(year)
				}
			else {
				reshape long stock, i(reportCtyName) j(year)
				}
			
		}
		
		else if A[1]=="Region / economy" {
			gen reportCtyName = "`ctyName'"
			
			*** the format of region / country is more complicated
			*** first find the first year (2001) in line 1
			quietly ds source notes reportCtyName, not
			local countryNameList ""
			foreach x in `r(varlist)' {
				if real(`x'[1])==. {
					local countryNameList `countryNameList' `x'
				}
				else if real(`x'[1])>=2001 & real(`x'[1])<=2012 {
					local yr = `x'[1]
					if "`sheetName'"=="inflows" | "`sheetName'"=="outflows" {
						ren `x' flow`yr'
						}
					else {
						ren `x' stock`yr'
						}
					}
				else {
					display "The column `x' is real in line 1 but not between 2001 and 2012. Check."
					error 1
					}
				}			
			
			drop if A=="Region / economy"
			
			egen numnonmiss_cty = rownonmiss(`countryNameList'), strok
			drop if numnonmiss_cty==0
			quietly sum numnonmiss_cty
			if `r(max)'>1 {
				disp "more than one country name in some row, check `ctyCode' `sheetName'."
				error 1
				}
			
			if "`sheetName'"=="inflows" | "`sheetName'"=="instock" {
				gen ctyName_o = ""
				foreach y in `countryNameList' {
					replace ctyName_o = `y' if missing(ctyName_o)
					}
				
				count if ctyName_o==".."
				if `r(N)'>0 {
					disp "some origin country is .. for `sheetName' of country `ctyCode', check."
					error 1
					}
				
				gen ctyName_d = "`ctyName'"
				gen iso3_d = "`ctyCode'"
				
				if "`sheetName'"=="inflows" {
					reshape long flow, i(ctyName_o) j(year)
					}
				else {
					reshape long stock, i(ctyName_o) j(year)
					}				
			}
			else {
				gen ctyName_d = ""
				foreach y in `countryNameList' {
					replace ctyName_d = `y' if missing(ctyName_d)
					}
				
				count if ctyName_d==".."
				if `r(N)'>0 {
					disp "some destination country is .. for `sheetName' of country `ctyCode', check."
					error 1
					}	
				
				gen ctyName_o = "`ctyName'"
				gen iso3_o = "`ctyCode'"
				
				if "`sheetName'"=="outflows" {
					reshape long flow, i(ctyName_d) j(year)
					}
				else {
					reshape long stock, i(ctyName_d) j(year)
					}	
			}			
			
			drop `countryNameList' numnonmiss_cty
		}
		
		else {
			display "the table is of neither format, check `ctyCode'."
			error 1
		}
		
		save ``sheetName'', replace
		
	}
			
	clear
	append using `inflows'
	append using `outflows'
	ren (notes source) (flow_notes flow_source)
	tempfile flows
	save `flows', replace
	
	clear 
	append using `instock'
	append using `outstock'
	ren (notes source) (stock_notes stock_source)
	merge 1:1 reportCtyName ctyName_o ctyName_d year using `flows', nogen
	save "processed_data/temp/UNCTAD_raw_`ctyCode'.dta", replace

}


clear
local fileList : dir "${PATH_IN_DATA}/" files "UNCTAD_raw_*.dta", respectcase
foreach f in `fileList' {
	append using "${PATH_IN_DATA}/`f'"
	erase "${PATH_IN_DATA}/`f'"
	}

** destring
foreach x of varlist stock flow {
	replace `x' = "0" if `x'=="-"
	replace `x' = "" if `x'==".."
	}
destring stock flow, replace
drop if missing(flow) & missing(stock)	

compress
save "${PATH_IN_DATA}/UNCTAD_raw.dta", replace
