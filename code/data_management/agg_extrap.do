**********************************************
** extrapolate aggregate level data to 2012
**********************************************

set graphics off

local outputPath = "$tableDir/agg_extra_to_2012.csv"
capture rm "`outputPath'"

program drop _all
program countCountry
    egen temp = tag(iso3) if e(sample)
    quietly sum temp
    estadd scalar num_cty = r(sum)
    drop temp
end


use countrycode year emp using "source_data/PennWorldTable/8.0/pwt80.dta", clear
ren countrycode iso3
merge 1:1 iso3 year using "processed_data/gdp.dta", nogen
merge 1:1 iso3 year using "processed_data/nonfin_output_share.dta", nogen

keep if year>=1990
fillin iso3 year
encode iso3, gen(id_iso3)
xtset id_iso3 year

** Plot output to GDP ratio

local figureFolder = "$figureDir/output_to_gdp"
capture mkdir "`figureFolder'"
gen nonfin_output_to_gdp = nonfin_output / gdp 
egen num_nonmiss = total(gdp<. & nonfin_output<.), by(iso3)
/*
quietly levelsof iso3 if num_nonmiss > 0
foreach cty in `r(levels)' {
    twoway (tsline nonfin_output_to_gdp if iso3=="`cty'")
    graph export "`figureFolder'/`cty'.pdf", replace
}
*/

** extrapolate output
egen tag_iso3 = tag(iso3)
estpost tabulate num_nonmiss if tag_iso3, sort
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
    title("number of nonmissing gdp and nonfinancial output since 1990")

estpost tabulate year if nonfin_output<.
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Number of countries with nonmissing nonfinancial output in each year") ///
            eqlabels(, lhs("year \ iso3_o"))


gen log_output = log(nonfin_output)
gen log_gdp = log(gdp)

** estimate output-gdp relationship by year range
eststo clear
forvalues yr = 1990(6)2010 {
    local endYr = `yr' + 5
    quietly eststo: reg log_output log_gdp if inrange(year,`yr',`endYr'), ro
    countCountry
    estadd local yr_range "`yr'-`endYr'"
}
esttab * using "`outputPath'", append se r2 nogaps ///
    scalars("num_cty Num of countries" "yr_range year range" ) ///
    title("Estimate extrapolation equation every 6 years")

gen yr_range = "1990-1995" if inrange(year,1990,1995)
replace yr_range = "1996-2001" if inrange(year,1996,2001)
replace yr_range = "2002-2013" if inrange(year,2002,2013)
encode yr_range, gen(id_yr_range)

** extrapolation
eststo clear
quietly eststo: reg log_output i.id_yr_range i.id_yr_range#c.log_gdp, ro
esttab * using "`outputPath'", append se r2 nogaps ///
    title("Estimate extrapolation equation 1990 - 2013")
predict temp, xb
gen nonfin_output_extrap = nonfin_output
replace nonfin_output_extrap = exp(temp) if missing(nonfin_output_extrap)
label var nonfin_output "Nonfinancial total gross output, sources: STAN and KLEMS"
label var nonfin_output_extrap "Nonfinancial total gross output, extrapolated using GDP"

** a more flexible regression    
quietly eststo: reg log_output i.year i.year#c.log_gdp, ro    
tempfile output_gdp
parmest, saving(`output_gdp',replace)

regress log_output log_gdp if inrange(year,1990,2007)
local a_one_coef = _b[_cons]
local b_one_coef = _b[log_gdp]
drop num_nonmiss temp log_*

** extrapolate employment, constant growth model
gen log_emp = log(emp)
egen num_nonmiss = total(log_emp<.), by(iso3)
quietly reg log_emp i.id_iso3 i.id_iso3#c.year if num_nonmiss >= 10
predict temp, xb
replace emp = exp(temp) if missing(emp) & num_nonmiss >= 10

drop num_nonmiss temp log_*

keep iso3 year emp gdp nonfin_output output_source nonfin_output_share nonfin_output_extrap
compress
sort iso3 year
save "processed_data/agg_extrap.dta", replace


*** additional output: goodness of fit of the log-linear extrapolation of gdp to nonfinancial output

use `output_gdp', clear
split parm, parse("#")

quietly sum estimate if parm=="_cons"
local a0 = r(mean)

preserve
keep if regexm(parm2,"log_gdp")
ren estimate b
gen year = regexs(1) if regexm(parm1,"^([0-9]+)")
destring year, replace
keep year b
tempfile b
save `b', replace
restore

keep if missing(parm2) & parm1 ~= "_cons"
ren estimate a
replace a = a + `a0'
gen year = regexs(1) if regexm(parm1,"^([0-9]+)")
destring year, replace
keep year a
merge 1:1 year using `b'
quietly sum b
twoway connected b year, yline(`b_one_coef') yline(`r(mean)',lpattern(dash_dot)) ///
    title("coefficient before log(gdp)") ///
    note("solid horizontal line is estimated common coefficient, dashed line is average b_t")
graph export "$figureDir/extrap_output_panel_b.eps", replace
    
quietly sum a
twoway connected a year, yline(`a_one_coef') yline(`r(mean)',lpattern(dash_dot)) ///
    title("constant by year") ///
    note("solid horizontal line is estimated common constant, dashed line is average a_t")
graph export "$figureDir/extrap_output_panel_a.eps", replace
set graphics on
