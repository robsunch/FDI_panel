********************************************
** construct variables needed for 
** exchange rate adjustments
********************************************

local outputPath = "$tableDir/exchRate.csv"
capture rm "`outputPath'"

*** EUR to USD exchange rate
import excel date euroExchRate using "source_data/misc/DEXUSEU.xls", clear cellrange(a15:b30)
gen year = year(date)
tempfile euroExchRate
save `euroExchRate', replace

*** Euro fixed rates
import excel using "source_data/misc/euroFixedRate.xlsx", clear firstrow
gen year_of_adoption = year(date_of_adoption)
tempfile euroFixedRate
save `euroFixedRate', replace

*** OECD exchange rates
insheet using "source_data/misc/OECD_SNA_TABLE4.csv", clear names
keep if transact == "EXC"
ren (location unitcode unit value) (iso3 currency_oecd des_currency_oecd exchRate_oecd)
keep iso3 year currency_oecd des_currency_oecd exchRate_oecd
tempfile exchRate_oecd
save `exchRate_oecd', replace

*** Eurostat exchange rates
import excel using "source_data/eurostat/fats_esms_an1.xlsx", sheet("currency_iso3") clear firstrow
tempfile currency_iso3
save `currency_iso3', replace
import excel using "source_data/eurostat/fats_esms_an1.xlsx", sheet("Table 1") clear allstring
drop in 1/2
ren A currency
ds currency, not
foreach x in `r(varlist)' {
    local newName = "lcu_to_eur_es" + `x'[1]
    ren `x' `newName'
}
drop in 1
reshape long lcu_to_eur_es, i(currency) j(year)
gen lcu_to_eur_es_with_brackets = regexm(lcu_to_eur_es,"^\[")
replace lcu_to_eur_es = subinstr(lcu_to_eur_es,",",".",1)
destring lcu_to_eur_es, ignore("-[]") replace
merge m:1 currency using `currency_iso3', keep(master match) nogen
ren currency lcu
compress
tempfile lcu_to_eur_es
save `lcu_to_eur_es', replace

*** World Bank official exchange rates
import excel using "source_data/misc/pa.nus.fcrf_Indicator_en_excel_v2.xls", sheet("Data") cellrange(a3:bf261) firstrow clear
drop Indicator* CountryName
ren CountryCode iso3
quietly ds iso3, not
foreach x of varlist `r(varlist)' {
    local xlab : var label `x'
    ren `x' year`xlab'
    }
reshape long year, i(iso3) j(yr)
ren year exchRate_wdi
ren yr year

merge m:1 iso3 using `euroFixedRate', nogen
merge m:1 year using `euroExchRate', keep(master match) nogen
ren iso3 countrycode
merge m:1 countrycode year using "source_data/PennWorldTable/8.0/pwt80.dta", keep(master match) nogen keepusing(xr)
ren (countrycode xr) (iso3 exchRate_pwt)
merge 1:1 iso3 year using `exchRate_oecd', keep(master match) nogen
merge 1:1 iso3 year using `lcu_to_eur_es', keep(master match) nogen

keep iso3 year year_of_adoption exchRate_* fixedRate euroExchRate lcu_to_eur_es lcu currency_oecd
label var euroExchRate "Euro exchange rate USD : 1 EUR"
label var fixedRate "Euro fixed rate LCU : 1 EUR"
label var exchRate_wdi "WDI exchange rate LCU : 1 USD"
label var exchRate_pwt "PWT exchange rate LCU : 1 USD"
label var exchRate_oecd "OECD exchange rate LCU : 1 USD"
label var lcu_to_eur_es "Eurostat LCU : 1 EUR"
label var lcu "Currency (historical for EMU countries)"
sort iso3 year
compress
save "processed_data/exchRate.dta", replace

*** infer the methods of OECD and Eurostat exchange (or conversion) rates
gen temp = fixedRate / exchRate_wdi 
gen diff_temp_pwt = temp / exchRate_wdi - 1
gen diff_temp_oecd = temp / exchRate_oecd - 1



