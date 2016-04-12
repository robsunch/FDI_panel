**************************************************
** import Russian data from World KLEMS
**
** input: source_data/worldKlems/extraCountries/RUS_wk_JULY_2013.csv
**
**************************************************


*** import Russian data, similar format as EU Klems
insheet using "source_data/worldKlems/extraCountries/RUS_wk_JULY_2013.csv", clear case names
ren (code Variable) (ind var)
keep if inlist(var,"GO","VA") & inlist(ind, "D","TOT","J")
drop des
gen iso3 = "RUS"
destring _*, ignore(",") replace
reshape long _, i(iso3 ind var) j(year)
ren _ value

reshape wide value, i(iso3 var year) j(ind) string
ren valueD manu
gen nonfin_total = valueTOT-valueJ
ren valueTOT tot_
drop value*
reshape wide manu nonfin_total tot_, i(iso3 year) j(var) string

