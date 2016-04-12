*****************************************************
** this do-file combines all data from World Klems on total output
**
** input: source_data/worldKlems/transform_txt.xlsx
**       .../PennWorldTable/8.0/pwt80.dta
**       .../Misc/euroFixedRate.xlsx
**
** need written do-files: $codeDir/XXX_world_klems.do where
**                  XXX = can, chn, rus, twn
**
** output: processed_data/worldKlems_va_go.dta
**
*****************************************************

*** for countries adopting Euro between 2008 and 2011, PWT uses the Euro fixed rates to obtain "exchange rates"
*** while Klems use local currency: SVK, CYP, MLT, EST (see "source_data/worldKlems/euroByCountry")
*** need adjustment later on
import excel using "source_data/misc/euroFixedRate.xlsx", sheet("Sheet1") firstrow clear
keep if year(date_of_adoption) >= 2008 & year(date_of_adoption) <= 2011
keep iso3 fixedRate
tempfile fixedRate
save `fixedRate', replace


*****************************************
*** check World Klems, add another four countries
*****************************************
foreach cty in chn rus twn can {
    do "$codeDir/world_klems_`cty'.do"
    tempfile `cty'
    save ``cty'', replace
    }

***** EU Klems data
import excel using "source_data/worldKlems/transform_txt.xlsx", sheet("basic") clear firstrow 
** deal with the quotes
foreach x of varlist country _2006 _2007 {
    replace `x' = subinstr(`x',`"""',"",1)
    }
destring _2006 _2007, replace

ren (country code) (iso3 ind)
replace iso3 = "USA" if iso3=="USA-NAICS"
replace iso3 = "DEU" if iso3=="GER"
replace iso3 = "GBR" if iso3=="UK"

reshape long _, i(iso3 ind var) j(year)
ren _ value
keep if inlist(var,"GO","VA") & inlist(ind, "D","TOT","J")

reshape wide value, i(iso3 var year) j(ind) string
ren valueD manu
gen nonfin_total = valueTOT-valueJ
ren valueTOT tot_
drop value*

reshape wide manu nonfin_total tot_, i(iso3 year) j(var) string

*** append three countries that are in World Klems but not EU Klems
foreach cty in chn rus twn can{
    append using ``cty''
    }

ren iso3 countrycode
merge 1:1 countrycode year using "source_data/PennWorldTable/8.0/pwt80.dta", keep(master match) keepusing(xr) nogen
ren countrycode iso3
merge m:1 iso3 using `fixedRate', nogen


foreach x of varlist manu* tot_* nonfin_* {
    replace `x' = `x' / fixedRate if ~missing(fixedRate) // LCU to "euros" before adoption
    replace `x' = `x' * 1e6 / xr // from million LCU (including before-adoption euros) to dollars
    ren `x' `x'_Klems
    }
drop xr fixedRate
label var manuGO_Klems "KLEMS manufacturing gross output, current USD"
label var tot_GO_Klems "KLEMS total gross output, current USD"
label var nonfin_totalGO_Klems "KLEMS nonfinancial total gross output, current USD"
label var manuVA_Klems "KLEMS manufacturing value added, current USD"
label var tot_VA_Klems "KLEMS total value added, current USD"
label var nonfin_totalVA_Klems "KLEMS nonfinancial value added, current USD"
compress
save "processed_data/worldKlems_va_go.dta", replace 

