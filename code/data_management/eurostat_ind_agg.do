*****************************************************
** this do-file imports Eurostat bilateral affiliate sales, 
** FDI flows and FDI stocks data (industry aggregate)
*****************************************************

log close _all
log using "$logDir/eurostat_ind_agg.smcl", replace

local data_in = "processed_data/temp/eurostat"
local data_out = "processed_data/eurostat"
capture mkdir "`data_out'"
local exchRateList exchRate_wdi lcu_to_eur_es euroExchRate fixedRate year_of_adoption
local outputPath = "$tableDir/eurostat_ind_agg.csv"
capture rm `outputPath'

program drop _all

program checkMiss
    display "check causes for missing values"
    tab value2 if missing(value1), missing sort // causes for missing values
    display "check flags for nonmissing values"
    tab value2 if ~missing(value1), missing sort // flags for nonmissing values
end

program adj_exch_rate_es_fats
    syntax [varlist]
    foreach x in `varlist' {
        gen temp = .
        replace temp = `x' * euroExchRate * 1e6 if year>=1999
        replace temp = `x' * lcu_to_eur_es / exchRate_wdi * 1e6 if year<1999 // change to LCU then use WDI exchange rates
        count if temp==. & `x'<.
        if `r(N)'==0 {
            drop `x'
            ren temp `x'
            label var `x' "Unit: USD"
        }
        else {
            display as error "Exchange rate adjustments did not cover some obs for `x'. Check."
            tab year iso3 if temp==. & `x'<.
            error 1
        }
    }
end

program adj_exch_rate_es_fdi
    syntax [varlist]
    foreach x in `varlist' {
        gen temp = .
        replace temp = `x' * euroExchRate * 1e6 // before 1999 ECU; after 1999 EURO
        count if temp==. & `x'<.
        if `r(N)'==0 {
            drop `x'
            ren temp `x'
            label var `x' "Unit: USD"
        }
        else {
            display as error "Exchange rate adjustments did not cover some obs for `x'. Check."
            tab year iso3 if temp==. & `x'<.
            error 1
        }
    }
end

****************
*** dictionary files
****************
foreach x in geo indic_sb indic_bp nace_r2 nace_r1 {
    insheet `x' `x'_des using "source_data/eurostat/dic/`x'.dic", tab clear
    tempfile `x'
    save ``x'', replace
}

** selected variables for fats
import excel using "processed_data/select_var.xlsx", clear firstrow sheet("eurostat_indic_sb_output")
tempfile selected_var_in
save `selected_var_in', replace
import excel using "processed_data/select_var.xlsx", clear firstrow sheet("eurostat_indic_bp_output")
drop if missing(varName)
tempfile selected_var_out
save `selected_var_out', replace

*****************************
** fats tables industry aggregate
*****************************
use if nace_r2 == "B-N_S95_X_K" using "`data_in'/fats_g1b_08.dta", clear
// INDUSTRY: Total business economy; repair of computers, personal and household goods; except financial and insurance activities
merge m:1 indic_sb using `selected_var_in', keep(match) keepusing(indic_sb varName) nogen 
drop nace_r2 indic_sb
reshape long value, i(geo c_ctrl varName) j(year)
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values
checkMiss
destring value1, replace
ren (value1 value2) (value flag)
reshape wide value flag, i(geo c_ctrl year) j(varName) string
ren value* *_totXfin
ren flag* flag_*_totXfin

tempfile fats_g1b_08
save `fats_g1b_08', replace

use if nace_r1=="C-K_X_J" using "`data_in'/fats_g1b_03.dta", clear
// INDUSTRY: C-K_X_J    Business economy - Industry and services (except financial intermediation)
merge m:1 indic_sb using `selected_var_in', keep(match) keepusing(indic_sb varName) nogen 
drop nace_r1 indic_sb
reshape long value, i(geo c_ctrl varName) j(year)
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values
checkMiss
destring value1, replace
ren (value1 value2) (value flag)
reshape wide value flag, i(geo c_ctrl year) j(varName) string
ren value* *_totXfin
ren flag* flag_*_totXfin

tempfile fats_g1b_03
save `fats_g1b_03', replace

*** fats 96 tables
local fileList : dir "`data_in'/fats_96/" files "fats_*.dta"
foreach f in `fileList' {
    local iso2 = subinstr(subinstr("`f'",".dta","",.),"fats_","",.)
    if "`f'"~="fats_ie.dta" & "`f'"~="fats_de.dta" {
        use if nace_r1=="C-K_X_J" using "`data_in'/fats_96/`f'", clear
        merge m:1 indic_sb using `selected_var_in', keep(match) keepusing(indic_sb varName) nogen 
        drop nace_r1 indic_sb
        reshape long value, i(geo c_ctrl varName) j(year)
        split value, parse(" ")
        drop value
        replace value1 = "" if value1==":"
        destring value1, replace
        ren (value1 value2) (value flag)
        }
    else {
        use if length(nace_r1)==1 & nace_r1~="J" using "`data_in'/fats_96/`f'", clear
        merge m:1 indic_sb using `selected_var_in', keep(match) keepusing(indic_sb varName) nogen
        reshape long value, i(geo c_ctrl nace_r1 varName) j(year)
        split value, parse(" ")
        drop value indic_sb
        replace value1 = "" if value1==":"
        destring value1, replace
        ** aggregate over sectors: C D E G H I K to mimic C-K_X_J
        ** treats combinations of geo * c_ctrl * nace_r1 * year * varName
        ** not in the original dataset as zero, if any sector has missing values
        ** in the original dataset, the aggregate value will be missing
        fillin geo c_ctrl nace_r1 year varName
        egen anymiss = total(value1==. & _fillin==0), by(geo c_ctrl year varName)
        keep if anymiss==0
        collapse (sum) value = value1, by(geo c_ctrl year varName)
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
merge 1:1 geo c_ctrl year varName using `reshaped_sum', update nogen
    
reshape wide value flag, i(geo c_ctrl year) j(varName) string
ren value* *_totXfin
ren flag* flag_*_totXfin

append using `fats_g1b_08'
append using `fats_g1b_03'

** standardize iso codes
replace geo = "GB" if geo == "UK"
replace geo = "GR" if geo == "EL"
ren geo iso2
merge m:1 iso2 using "processed_data/isoStandard.dta", keepusing(iso3) keep(match) nogen
ren iso2 geo
** exchange rate adjustment
merge m:1 iso3 year using "processed_data/exchRate.dta", keep(master match) nogen ///
    keepusing(`exchRateList')
adj_exch_rate_es_fats inv_tangi* psn_cost* purchase* rev* vadd* 
drop iso3 `exchRateList'

*** check growth rates for outliers
egen id_pair = group(geo c_ctrl)
xtset id_pair year
ds *, has(type numeric)
foreach x in `r(varlist)' {
    gen diff_log_`x' = log(`x') - log(l.`x')
}
estpost summarize diff_log_*, quietly detail
esttab . using "`outputPath'", append cells("count(fmt(a3)) mean sd min p1 p5 p10 p25 p50 p75 p90 p95 p99 max") noobs ///
    title("summary stats for log growth rates - inward fats")
estpost tabstat diff_log_*, by(year) statistics(mean count) ///
    columns(statistics) quietly
esttab . using "`outputPath'", append main(mean) aux(count) nogap nostar ///
    unstack noobs nonote label ///
    title("mean (N) log growth rates - inward fats")    
drop diff_log_* id_pair

compress
duplicates report year geo c_ctrl
duplicates drop
save "`data_out'/fats_in.dta", replace

*****************
*** outward sales
*****************
** nace r1 for earlier years
foreach f in fats_out1 fats_out2 {
    use if nace_r1=="J" using "`data_in'/`f'.dta", clear
    merge m:1 indic_bp using `selected_var_out', keep(match) keepusing(indic_bp varName) nogen
    drop nace_r1 indic_bp
    reshape long value, i(geo partner varName) j(year)
    split value, parse(" ") // flags are separated by a space
    drop value
    replace value1 = "" if value1==":" // missing values
    checkMiss
    destring value1, replace
    ren (value1 value2) (value flag)
    reshape wide value flag, i(geo partner year) j(varName) string
    ren value* *_fin
    ren flag* flag_*_fin
    tempfile `f'_fin
    save ``f'_fin', replace

    use if nace_r1=="A-O_X_L" using "`data_in'/`f'.dta", clear
    // INDUSTRY: A-O_X_L All NACE activities (except public administration; activities of households and extra-territorial organizations)
    merge m:1 indic_bp using `selected_var_out', keep(match) keepusing(indic_bp varName) nogen
    drop nace_r1 indic_bp
    reshape long value, i(geo partner varName) j(year)
    split value, parse(" ") // flags are separated by a space
    drop value
    replace value1 = "" if value1==":" // missing values
    checkMiss
    destring value1, replace
    ren (value1 value2) (value flag)
    reshape wide value flag, i(geo partner year) j(varName) string
    ren value* *_tot
    ren flag* flag_*_tot
    merge 1:1 geo partner year using ``f'_fin', nogen
    tempfile `f'
    save ``f'', replace
}

** nace r2 for late years
use if nace_r2=="K" using "`data_in'/fats_out2_r2.dta", clear
merge m:1 indic_bp using `selected_var_out', keep(match) keepusing(indic_bp varName) nogen
drop nace_r2 indic_bp
reshape long value, i(geo partner varName) j(year)
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values
checkMiss
destring value1, replace
ren (value1 value2) (value flag)
reshape wide value flag, i(geo partner year) j(varName) string
ren value* *_fin
ren flag* flag_*_fin
tempfile fats_out2_r2_fin
save `fats_out2_r2_fin', replace

use if nace_r2=="B-S_X_O" using "`data_in'/fats_out2_r2.dta", clear
// INDUSTRY: B-S_X_O Industry, construction and services (except public administration, defense, compulsory social security)
merge m:1 indic_bp using `selected_var_out', keep(match) keepusing(indic_bp varName) nogen
drop nace_r2 indic_bp
reshape long value, i(geo partner varName) j(year)
split value, parse(" ") // flags are separated by a space
drop value
replace value1 = "" if value1==":" // missing values
checkMiss
destring value1, replace
ren (value1 value2) (value flag)
reshape wide value flag, i(geo partner year) j(varName) string
ren value* *_tot
ren flag* flag_*_tot
merge 1:1 geo partner year using `fats_out2_r2_fin', nogen
tempfile fats_out2_r2
save `fats_out2_r2', replace

*** append other years
clear
foreach f in fats_out1 fats_out2 fats_out2_r2 {
append using ``f''
}
duplicates report geo partner year
duplicates drop

** standardize iso codes
replace geo = "GB" if geo == "UK"
replace geo = "GR" if geo == "EL"
ren geo iso2
merge m:1 iso2 using "processed_data/isoStandard.dta", keepusing(iso3) keep(match) nogen
ren iso2 geo
** exchange rate adjustment
merge m:1 iso3 year using "processed_data/exchRate.dta", keep(master match) nogen ///
    keepusing(`exchRateList')
adj_exch_rate_es_fats rev*
drop iso3 `exchRateList'

*** check growth rates for outliers
egen id_pair = group(geo partner)
xtset id_pair year
ds *, has(type numeric)
foreach x in `r(varlist)' {
    gen diff_log_`x' = log(`x') - log(l.`x')
}
estpost summarize diff_log_*, quietly detail
esttab . using "`outputPath'", append cells("count(fmt(a3)) mean sd min p1 p5 p10 p25 p50 p75 p90 p95 p99 max") noobs ///
    title("summary stats for log growth rates - outward fats")
estpost tabstat diff_log_*, by(year) statistics(mean count) ///
    columns(statistics) quietly
esttab . using "`outputPath'", append main(mean) aux(count) nogap nostar ///
    unstack noobs nonote label ///
    title("mean (N) log growth rates - outward fats")    
drop diff_log_* id_pair

compress
save "`data_out'/fats_out.dta", replace

**********************************
** inward flow and stocks by main countries
** TEC tables
**********************************
use "`data_in'/tec00049.dta", clear
reshape long value, i(geo partner) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
ren (value2 value1 geo partner) (eurostat_in_flow_flag eurostat_in_flow iso2_d iso2_o)
keep iso2_d iso2_o year eurostat_in_flow_flag eurostat_in_flow
tempfile eurostat_in_flow
save `eurostat_in_flow', replace

use "`data_in'/tec00051.dta", clear
reshape long value, i(geo partner) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
ren (value2 value1 geo partner) (eurostat_in_stock_flag eurostat_in_stock iso2_d iso2_o)
keep iso2_d iso2_o year eurostat_in_stock_flag eurostat_in_stock
tempfile eurostat_in_stock
save `eurostat_in_stock', replace

**********************************
** outward flow and stocks by main countries
**********************************

use "`data_in'/tec00053.dta", clear
reshape long value, i(geo partner) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1 geo partner) (eurostat_out_flow_flag eurostat_out_flow iso2_o iso2_d)
keep iso2_o iso2_d year eurostat_out_flow_flag eurostat_out_flow
tempfile eurostat_out_flow
save `eurostat_out_flow', replace

use "`data_in'/tec00052.dta", clear
reshape long value, i(geo partner) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
ren (value2 value1 geo partner) (eurostat_out_stock_flag eurostat_out_stock iso2_o iso2_d)
keep iso2_o iso2_d year eurostat_out_stock_flag eurostat_out_stock

merge 1:1 iso2_o iso2_d year using `eurostat_out_flow', nogen
merge 1:1 iso2_o iso2_d year using `eurostat_in_flow', nogen
merge 1:1 iso2_o iso2_d year using `eurostat_in_stock', nogen

** exchange rate adjustment
foreach direc in in out {
    if "`direc'" == "in" {
        local x d
    }
    else {
        local x o
    }
    ren iso2_`x' iso2
    replace iso2 = "GB" if iso2 == "UK"
    replace iso2 = "GR" if iso2 == "EL"
    replace iso2 = "CN" if iso2 == "CN_X_HK"
    merge m:1 iso2 using "processed_data/isoStandard.dta", keep(match) keepusing(iso3) nogen
    ren iso2 iso2_`x'
    merge m:1 iso3 year using "processed_data/exchRate.dta", keep(master match) nogen ///
    keepusing(euroExchRate)
    adj_exch_rate_es_fdi eurostat_`direc'_stock eurostat_`direc'_flow
    drop iso3 euroExchRate
}

compress
duplicates report iso2_o iso2_d year
save "`data_out'/tec_stock_flow.dta", replace

*****************************************
** BOP tables
*****************************************
use if nace_r2=="TOT_FDI" & inlist(post,"505","555") using "`data_in'/bop_fdi_flow_r2.dta", clear
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
    (eurostat_out_flow_r2 eurostat_in_flow_r2 eurostat_out_flow_r2_flag eurostat_in_flow_r2_flag)
preserve
keep geo partner year eurostat_in_*
ren (geo partner) (iso2_d iso2_o)
tempfile eurostat_in_flow_r2
save `eurostat_in_flow_r2', replace
restore
keep geo partner year eurostat_out_*
ren (geo partner) (iso2_o iso2_d)
tempfile eurostat_out_flow_r2
save `eurostat_out_flow_r2', replace

use if nace_r1=="TOT_FDI" & inlist(post,"505","555") using "`data_in'/bop_fdi_flows.dta", clear
reshape long value, i(geo partner post) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1) (flow_flag flow)
reshape wide flow flow_flag, i(geo partner year) j(post) string
ren (flow505 flow555 flow_flag505 flow_flag555) ///
    (eurostat_out_flow_r1 eurostat_in_flow_r1 eurostat_out_flow_r1_flag eurostat_in_flow_r1_flag)
preserve
keep geo partner year eurostat_in_*
ren (geo partner) (iso2_d iso2_o)
tempfile eurostat_in_flow_r1
save `eurostat_in_flow_r1', replace
restore
keep geo partner year eurostat_out_*
ren (geo partner) (iso2_o iso2_d)
tempfile eurostat_out_flow_r1
save `eurostat_out_flow_r1', replace

use if nace_r2=="TOT_FDI" & inlist(post,"505","555") using "`data_in'/bop_fdi_pos_r2.dta", clear
reshape long value, i(geo partner post) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1) (stock_flag stock)
reshape wide stock stock_flag, i(geo partner year) j(post) string
ren (stock505 stock555 stock_flag505 stock_flag555) ///
    (eurostat_out_stock_r2 eurostat_in_stock_r2 eurostat_out_stock_r2_flag eurostat_in_stock_r2_flag)
preserve
keep geo partner year eurostat_in_* 
ren (geo partner) (iso2_d iso2_o)
tempfile eurostat_in_stock_r2
save `eurostat_in_stock_r2', replace
restore
preserve
keep geo partner year eurostat_out_* 
ren (geo partner) (iso2_o iso2_d)
tempfile eurostat_out_stock_r2
save `eurostat_out_stock_r2', replace
restore


use if nace_r1=="TOT_FDI" & inlist(post,"505","555") using "`data_in'/bop_fdi_pos.dta", clear
reshape long value, i(geo partner post) j(year)
split value, parse(" ") // flags are separated by a space
checkMiss
replace value1 = "" if value1==":" // missing values
destring value1, replace
drop value
ren (value2 value1) (stock_flag stock)
reshape wide stock stock_flag, i(geo partner year) j(post) string
ren (stock505 stock555 stock_flag505 stock_flag555) ///
    (eurostat_out_stock_r1 eurostat_in_stock_r1 eurostat_out_stock_r1_flag eurostat_in_stock_r1_flag)
preserve
keep geo partner year eurostat_in_* 
ren (geo partner) (iso2_d iso2_o)
tempfile eurostat_in_stock_r1
save `eurostat_in_stock_r1', replace
restore
preserve
keep geo partner year eurostat_out_* 
ren (geo partner) (iso2_o iso2_d)
tempfile eurostat_out_stock_r1
save `eurostat_out_stock_r1', replace
restore

*** combine BOP tables
clear
forvalues i = 1/2 {
    use `eurostat_in_flow_r`i''
    merge 1:1 iso2_o iso2_d year using `eurostat_out_flow_r`i'', nogen
    merge 1:1 iso2_o iso2_d year using `eurostat_in_stock_r`i'', nogen
    merge 1:1 iso2_o iso2_d year using `eurostat_out_stock_r`i'', nogen
    tempfile eurostat_stock_flow_r`i'
    save `eurostat_stock_flow_r`i'', replace
}

use `eurostat_stock_flow_r2', clear
merge 1:1 iso2_o iso2_d year using `eurostat_stock_flow_r1', nogen

** exchange rate adjustment
foreach direc in in out {
    if "`direc'" == "in" {
        local x d
    }
    else {
        local x o
    }
    ren iso2_`x' iso2
    replace iso2 = "GB" if iso2 == "UK"
    replace iso2 = "GR" if iso2 == "EL"
    replace iso2 = "CN" if iso2 == "CN_X_HK"
    merge m:1 iso2 using "processed_data/isoStandard.dta", keep(match) keepusing(iso3) nogen
    ren iso2 iso2_`x'
    merge m:1 iso3 year using "processed_data/exchRate.dta", keep(master match) nogen ///
    keepusing(euroExchRate)
    adj_exch_rate_es_fdi eurostat_`direc'_stock_r? eurostat_`direc'_flow_r?
    drop iso3 euroExchRate
}

compress
duplicates report iso2_o iso2_d year
save "`data_out'/bop_stock_flow", replace
    


