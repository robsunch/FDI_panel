****************************************************
** this do-file combines inward and outward flows and stocks 
** from 3 sources: UNCTAD, OECD and Eurostat
****************************************************

log close _all
log using "$logDir/combine_OECD_es_unctad_flow_stock.smcl", replace

local outputPath = "$tableDir/combine_OECD_es_unctad_flow_stock.csv"
capture rm "`outputPath'"

********************************
** Eurostat FDI stats
********************************

*** Euro exchange rate
use year euroExchRate using "processed_data/exchRate.dta", clear
drop if missing(euroExchRate)
duplicates drop
tempfile euroExchRate
save `euroExchRate', replace

*** dictionary files for country name (eurostat)
foreach x in geo {
    insheet `x' `x'_des using "source_data/eurostat/dic/`x'.dic", tab clear
    ren (geo geo_des) (iso2 countryName)
    tempfile `x'_dic
    save ``x'_dic', replace
}

use "processed_data/eurostat/bop_stock_flow.dta", clear
foreach direc in in out {
foreach x in stock flow {
    gen bop_`direc'_`x' = eurostat_`direc'_`x'_r2
    gen source_`direc'_`x' = "r2" if bop_`direc'_`x'<.
    replace source_`direc'_`x' = "r1" if bop_`direc'_`x'==. & eurostat_`direc'_`x'_r1<.
    replace bop_`direc'_`x' = eurostat_`direc'_`x'_r1 if bop_`direc'_`x'==.
    estpost tabulate year source_`direc'_`x', missing elabels quietly
    esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
        title("Source of Eurostat BOP stats for variable `direc' `x' by year") ///
        eqlabels(, lhs("year \ source")) varlabels(`e(labels)')
}
}
keep iso2_o iso2_d year bop_*

merge 1:1 iso2_o iso2_d year using "processed_data/eurostat/tec_stock_flow.dta", nogen
ren eurostat_* tec_*

foreach direc in in out {
foreach x in stock flow {
    gen es_`direc'_`x' = bop_`direc'_`x'
    gen source_`direc'_`x' = "bop" if bop_`direc'_`x' < .
    replace source_`direc'_`x' = "tec" if tec_`direc'_`x' < . & es_`direc'_`x' == .
    replace es_`direc'_`x' = tec_`direc'_`x' if es_`direc'_`x' == .
    estpost tabulate year source_`direc'_`x', missing elabels quietly 
    esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
        title("Source of Eurostat stats (BOP vs TEC) for variable `direc' `x' by year") ///
        eqlabels(, lhs("year \ source")) varlabels(`e(labels)')    
}
}
keep iso2_o iso2_d year es_*

** standardize iso codes
foreach x in o d {
    ren iso2_`x' iso2
    replace iso2="GB" if iso2=="UK" // UK
    replace iso2="GR" if iso2=="EL" // Greece
    merge m:1 iso2 using `geo_dic', keep(master match) nogen
    merge m:1 iso2 using "processed_data/isoStandard.dta", keep(master match) keepusing(iso3)
    estpost tabulate countryName if _merge==1, elabels quietly sort
    esttab . using "`outputPath'", append cell(b) noobs nonumber nomtitle ///
        title("non-standard iso2 for direction `x' in Eurostat FDI stats") ///
        varlabels(`e(labels)')
    ren (iso2 iso3) (iso2_`x' iso3_`x')
    keep if _merge == 3
    drop _merge countryName iso2_`x'
}

** millions of EUR/ECU to USD
merge m:1 year using `euroExchRate', keep(master match) nogen
foreach x of varlist es_* {
    replace `x' = `x' * euroExchRate
}    
drop euroExchRate
tempfile es_fdi
save `es_fdi', replace

***********************
** OECD FDI flow/stock
***********************

*** standardized country code for OECD
import excel using "processed_data/isoStandard.xlsx", sheet("OECD_output") clear firstrow
drop if missing(_merge) // drop notes
drop if iso3=="WRT" | iso3=="WRX" // do not consider "world total" for stocks and flows
replace iso3 = iso3_OECD if missing(iso3)
keep iso3 iso3_OECD
duplicates drop
tempfile OECD_standard_ctyCode
save `OECD_standard_ctyCode', replace

use "processed_data/OECD/stock_flow.dta", clear
foreach x in o d {
    ren iso3_`x' iso3_OECD
    merge m:1 iso3_OECD using `OECD_standard_ctyCode', keepusing(iso3) keep(master match)   
    count if _merge==1
    if `r(N)' > 0 {
        estpost tabulate countryName_`x' if _merge==1, missing sort 
        esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Cases of non-standard iso3_`x' in OECD stock/flow data") ///
            varlabels(`e(labels)')
    }    
    keep if _merge==3
    drop _merge iso3_OECD
    ren iso3 iso3_`x'
}    

drop *flag countryName*

tempfile oecd_fdi
save `oecd_fdi', replace    

use "processed_data/UNCTAD_FDI.dta", clear
merge 1:1 iso3_o iso3_d year using `oecd_fdi', nogen
merge 1:1 iso3_o iso3_d year using `es_fdi', nogen

ds iso3_o iso3_d year, not
egen nonmiss = rownonmiss(`r(varlist)'), strok
drop if nonmiss == 0
drop nonmiss

compress
save "processed_data/OECD_ES_UNCTAD_flow_stock.dta", replace

log close _all
