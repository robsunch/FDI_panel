*****************************************************
** this do-file imports Eurostat bilateral affiliate sales, 
** FDI flows and FDI stocks data
*****************************************************

log close _all
log using "$logDir/import_eurostat.smcl", replace

** temporary directory structure
capture mkdir "processed_data/temp"
capture mkdir "processed_data/temp/fats_96"

program drop _all
program checkMiss
    display "check causes for missing values"
    tab value2 if missing(value1), sort // causes for missing values
    display "check flags for nonmissing values"
    tab value2 if ~missing(value1), sort // flags for nonmissing values
end

****************
*** dictionary files
****************
foreach x in geo indic_sb indic_bp nace_r2 nace_r1 {
    insheet `x' `x'_des using "source_data/eurostat/dic/`x'.dic", tab clear
    tempfile `x'
    save ``x'', replace
}

**************
** FATS tables
**************
*** fats_96
capture mkdir "processed_data/temp/fats_96/"
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

    save "processed_data/temp/fats_96/`fileName'", replace
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
    save "processed_data/temp/`fileName'", replace
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
    save "processed_data/temp/`fileName'", replace
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
    save "processed_data/temp/`fileName'", replace
}


*****************
*** inward sales
*****************
use if indic_sb=="V12110" & nace_r2=="B-N_S95_X_K" using "processed_data/temp/fats_g1b_08.dta", clear
// INDUSTRY: Total business economy; repair of computers, personal and household goods; except financial and insurance activities
// VARIABLE: Turnover or gross premiums written
drop nace_r2 indic_sb
reshape long value, i(geo c_ctrl) j(year)
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values

checkMiss

destring value1, replace
ren (value2 value1 geo c_ctrl) (eurostat_in_sales_flag eurostat_in_sales iso2_d iso2_o)
tempfile eurostat_in_sales_08
save `eurostat_in_sales_08', replace

use if indic_sb=="V12110" & nace_r1=="C-K_X_J" using "processed_data/temp/fats_g1b_03.dta", clear
// INDUSTRY: C-K_X_J    Business economy - Industry and services (except financial intermediation)
// VARIABLE: Turnover or gross premiums written
drop nace_r1 indic_sb
reshape long value, i(geo c_ctrl) j(year)
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values

checkMiss

destring value1, replace
ren (value2 value1 geo c_ctrl) (eurostat_in_sales_flag eurostat_in_sales iso2_d iso2_o)
tempfile eurostat_in_sales_03
save `eurostat_in_sales_03', replace

*** fats 96 tables
local fileList : dir "processed_data/temp/fats_96/" files "fats_*.dta"
foreach f in `fileList' {
    local iso2 = subinstr(subinstr("`f'",".dta","",.),"fats_","",.)
    if "`f'"~="fats_ie.dta" & "`f'"~="fats_de.dta" {
        use if indic_sb=="V12110" & nace_r1=="C-K_X_J" using "processed_data/temp/fats_96/`f'", clear
        drop nace_r1 indic_sb
        reshape long value, i(geo c_ctrl) j(year)
        split value, parse(" ")
        drop value
        replace value1 = "" if value1==":"
        destring value1, replace
        ren (value2 value1 geo c_ctrl) (eurostat_in_sales_flag eurostat_in_sales iso2_d iso2_o)
        }
    else {
        use if indic_sb=="V12110" & length(nace_r1)==1 & nace_r1~="J" using "processed_data/temp/fats_96/`f'", clear
        ** sectors: C D E G H I K
        reshape long value, i(geo c_ctrl nace_r1) j(year)
        split value, parse(" ")
        drop value indic_sb
        replace value1 = "" if value1==":"
        destring value1, replace
        fillin geo c_ctrl nace_r1 year
        egen anymiss = total(value1==.), by(geo c_ctrl year)
        keep if anymiss==0
        collapse (sum) value1, by(geo c_ctrl year)
        ren (value1 geo c_ctrl) (eurostat_in_sales iso2_d iso2_o)
        }
    tempfile reshaped_`iso2'
    save `reshaped_`iso2'', replace
}   

clear
foreach f in `fileList' {
    local iso2 = subinstr(subinstr("`f'",".dta","",.),"fats_","",.)
    if "`iso2'" ~= "sum" {
        append using `reshaped_`iso2''
        }
    }
merge 1:1 iso2_o iso2_d year using `reshaped_sum', update nogen

append using `eurostat_in_sales_03'
append using `eurostat_in_sales_08'

compress
duplicates drop
tempfile eurostat_in_sales
save `eurostat_in_sales', replace

*****************
*** outward sales
*****************
use if indic_bp=="TUR" & nace_r1=="A-O_X_L" using "processed_data/temp/fats_out1.dta", clear
// INDUSTRY: A-O_X_L All NACE activities (except public administration; activities of households and extra-territorial organizations)
// VARIABLE: Turnover - Million ECU/EUR
drop nace_r1 indic_bp
reshape long value, i(geo partner) j(year) string
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values

checkMiss

destring value1, replace
destring year, replace
ren (value2 value1 geo partner) (eurostat_out_sales_flag eurostat_out_sales iso2_o iso2_d)
tempfile eurostat_out_sales_04
save `eurostat_out_sales_04', replace

use if indic_bp=="TUR" & nace_r1=="J" using "processed_data/temp/fats_out1.dta", clear
drop nace_r1 indic_bp
reshape long value, i(geo partner) j(year) string
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values

checkMiss

destring value1, replace
destring year, replace
ren (value2 value1 geo partner) (eurostat_out_fin_sales_flag eurostat_out_fin_sales iso2_o iso2_d)
tempfile eurostat_out_fin_sales_04
save `eurostat_out_fin_sales_04', replace

use if indic_bp=="TUR" & nace_r1=="A-O_X_L" using "processed_data/temp/fats_out2.dta", clear
// INDUSTRY: A-O_X_L All NACE activities (except public administration; activities of households and extra-territorial organizations)
// VARIABLE: Turnover - Million ECU/EUR
drop nace_r1 indic_bp
reshape long value, i(geo partner) j(year) string
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values

checkMiss

destring value1, replace
destring year, replace
ren (value2 value1 geo partner) (eurostat_out_sales_flag eurostat_out_sales iso2_o iso2_d)
tempfile eurostat_out_sales_07
save `eurostat_out_sales_07', replace

use if indic_bp=="TUR" & nace_r1=="J" using "processed_data/temp/fats_out2.dta", clear
drop nace_r1 indic_bp
reshape long value, i(geo partner) j(year) string
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values

checkMiss

destring value1, replace
destring year, replace
ren (value2 value1 geo partner) (eurostat_out_fin_sales_flag eurostat_out_fin_sales iso2_o iso2_d)
tempfile eurostat_out_fin_sales_07
save `eurostat_out_fin_sales_07', replace

use if indic_bp=="TUR" & nace_r2=="B-S_X_O" using "processed_data/temp/fats_out2_r2.dta", clear
// INDUSTRY: B-S_X_O Industry, construction and services (except public administration, defense, compulsory social security)
// VARIABLE: Turnover - Million ECU/EUR
drop nace_r2 indic_bp
reshape long value, i(geo partner) j(year) string
split value, parse(" ") // flags are separated by a space
checkMiss
drop value
replace value1 = "" if value1==":" // missing values
destring value1, replace
destring year, replace
ren (value2 value1 geo partner) (eurostat_out_sales_flag eurostat_out_sales iso2_o iso2_d)
tempfile eurostat_out_sales_10
save `eurostat_out_sales_10', replace

use if indic_bp=="TUR" & nace_r2=="K" using "processed_data/temp/fats_out2_r2.dta", clear
drop nace_r2 indic_bp
reshape long value, i(geo partner) j(year) string
split value, parse(" ") // flags are separated by a space
/*
tab value2 if missing(value1), sort // causes for missing values
tab value2 if ~missing(value1), sort // flags for nonmissing values
*/
drop value
replace value1 = "" if value1==":" // missing values
destring value1, replace
destring year, replace
ren (value2 value1 geo partner) (eurostat_out_fin_sales_flag eurostat_out_fin_sales iso2_o iso2_d)
tempfile eurostat_out_fin_sales_10
save `eurostat_out_fin_sales_10', replace

*** append other years
use `eurostat_out_fin_sales_10', clear
append using `eurostat_out_fin_sales_04'
append using `eurostat_out_fin_sales_07'
tempfile eurostat_out_fin_sales
save `eurostat_out_fin_sales', replace


use `eurostat_out_sales_10', clear
append using `eurostat_out_sales_04'
append using `eurostat_out_sales_07'

merge 1:1 iso2_o iso2_d year using `eurostat_out_fin_sales', nogen
merge 1:1 iso2_o iso2_d year using `eurostat_in_sales', nogen

tempfile eurostat_sales
save `eurostat_sales', replace


**********************************
** inward flow and stocks by main countries
** TEC tables
**********************************
use "processed_data/temp/tec00049.dta", clear
reshape long value, i(geo partner) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
ren (value2 value1 geo partner) (eurostat_in_flow1_flag eurostat_in_flow1 iso2_d iso2_o)
keep iso2_d iso2_o year eurostat_in_flow1_flag eurostat_in_flow1
tempfile eurostat_in_flow1
save `eurostat_in_flow1', replace

use "processed_data/temp/tec00051.dta", clear
reshape long value, i(geo partner) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
ren (value2 value1 geo partner) (eurostat_in_stock1_flag eurostat_in_stock1 iso2_d iso2_o)
keep iso2_d iso2_o year eurostat_in_stock1_flag eurostat_in_stock1
tempfile eurostat_in_stock1
save `eurostat_in_stock1', replace

**********************************
** outward flow and stocks by main countries
**********************************

use "processed_data/temp/tec00053.dta", clear
reshape long value, i(geo partner) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1 geo partner) (eurostat_out_flow1_flag eurostat_out_flow1 iso2_o iso2_d)
keep iso2_o iso2_d year eurostat_out_flow1_flag eurostat_out_flow1
tempfile eurostat_out_flow1
save `eurostat_out_flow1', replace

use "processed_data/temp/tec00052.dta", clear
reshape long value, i(geo partner) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
ren (value2 value1 geo partner) (eurostat_out_stock1_flag eurostat_out_stock1 iso2_o iso2_d)
keep iso2_o iso2_d year eurostat_out_stock1_flag eurostat_out_stock1

merge 1:1 iso2_o iso2_d year using `eurostat_out_flow1', nogen
merge 1:1 iso2_o iso2_d year using `eurostat_in_flow1', nogen
merge 1:1 iso2_o iso2_d year using `eurostat_in_stock1', nogen

tempfile eurostat_stock_flow1
save `eurostat_stock_flow1', replace // EU inflow/outflow with rest of the world


*****************************************
** BOP tables
*****************************************
use if nace_r2=="TOT_FDI" & inlist(post,"505","555") using "processed_data/temp/bop_fdi_flow_r2.dta", clear
// All FDI activities, 505 for outward and 555 for inward
reshape long value, i(geo partner post) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1) (flow_flag flow)
reshape wide flow flow_flag, i(geo partner year) j(post) string
ren (flow505 flow555 flow_flag505 flow_flag555) ///
    (eurostat_out_flow2_r2 eurostat_in_flow2_r2 eurostat_out_flow2_r2_flag eurostat_in_flow2_r2_flag)
preserve
keep geo partner year eurostat_in_*
ren (geo partner) (iso2_d iso2_o)
tempfile eurostat_in_flow2_r2
save `eurostat_in_flow2_r2', replace
restore
keep geo partner year eurostat_out_*
ren (geo partner) (iso2_o iso2_d)
tempfile eurostat_out_flow2_r2
save `eurostat_out_flow2_r2', replace

use if nace_r1=="TOT_FDI" & inlist(post,"505","555") using "processed_data/temp/bop_fdi_flows.dta", clear
reshape long value, i(geo partner post) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1) (flow_flag flow)
reshape wide flow flow_flag, i(geo partner year) j(post) string
ren (flow505 flow555 flow_flag505 flow_flag555) ///
    (eurostat_out_flow2_r1 eurostat_in_flow2_r1 eurostat_out_flow2_r1_flag eurostat_in_flow2_r1_flag)
preserve
keep geo partner year eurostat_in_*
ren (geo partner) (iso2_d iso2_o)
tempfile eurostat_in_flow2_r1
save `eurostat_in_flow2_r1', replace
restore
keep geo partner year eurostat_out_*
ren (geo partner) (iso2_o iso2_d)
tempfile eurostat_out_flow2_r1
save `eurostat_out_flow2_r1', replace

use if nace_r2=="TOT_FDI" & inlist(post,"505","555") using "processed_data/temp/bop_fdi_pos_r2.dta", clear
reshape long value, i(geo partner post) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1) (stock_flag stock)
reshape wide stock stock_flag, i(geo partner year) j(post) string
ren (stock505 stock555 stock_flag505 stock_flag555) ///
    (eurostat_out_stock2_r2 eurostat_in_stock2_r2 eurostat_out_stock2_r2_flag eurostat_in_stock2_r2_flag)
preserve
keep geo partner year eurostat_in_* 
ren (geo partner) (iso2_d iso2_o)
tempfile eurostat_in_stock2_r2
save `eurostat_in_stock2_r2', replace
restore
preserve
keep geo partner year eurostat_out_* 
ren (geo partner) (iso2_o iso2_d)
tempfile eurostat_out_stock2_r2
save `eurostat_out_stock2_r2', replace
restore


use if nace_r1=="TOT_FDI" & inlist(post,"505","555") using "processed_data/temp/bop_fdi_pos.dta", clear
reshape long value, i(geo partner post) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1) (stock_flag stock)
reshape wide stock stock_flag, i(geo partner year) j(post) string
ren (stock505 stock555 stock_flag505 stock_flag555) ///
    (eurostat_out_stock eurostat_in_stock eurostat_out_stock_flag eurostat_in_stock_flag)
preserve
keep geo partner year eurostat_in_* 
ren (geo partner) (iso2_d iso2_o)
tempfile eurostat_in_stock2_r1
save `eurostat_in_stock2_r1', replace
restore
preserve
keep geo partner year eurostat_out_* 
ren (geo partner) (iso2_o iso2_d)
tempfile eurostat_out_stock2_r1
save `eurostat_out_stock2_r1', replace
restore

*** combine BOP tables
clear
forvalues i = 1/2 {
    use `eurostat_in_flow2_r`i''
    merge 1:1 iso2_o iso2_d year using `eurostat_out_flow2_r`i'', nogen
    merge 1:1 iso2_o iso2_d year using `eurostat_in_stock2_r`i'', nogen
    merge 1:1 iso2_o iso2_d year using `eurostat_out_stock2_r`i'', nogen
    tempfile eurostat_stock_flow2_r`i'
    save `eurostat_stock_flow2_r`i'', replace
}

use `eurostat_stock_flow2_r2', clear
merge 1:1 iso2_o iso2_d year using `eurostat_stock_flow2_r1', nogen
tempfile eurostat_stock_flow2
save `eurostat_stock_flow2', replace
    
*****************************
** merge flows and stocks from TEC
** and BOP tables
*****************************

use `eurostat_stock_flow2', clear
merge 1:1 iso2_d iso2_o year using `eurostat_stock_flow1', nogen
merge 1:1 iso2_o iso2_d year using `eurostat_sales', nogen

compress
save "processed_data/eurostat_FDI_raw.dta", replace

** remove temporary files
!rmdir "processed_data/temp/*" /q // to delete nonempty folders need to use shell commands


