*******************************************
** this do-file compare OECD country codes
** to standard codes
*******************************************

*** OECD country codes
local fileList AMNE_OUT4_world_total AMNE_OUT4_main_sectors FATS_OUT3_main_sectors AMNE_IN4_main_sectors FATS_IN3_main_sectors

foreach f of local fileList {
    use iso3_o countryName_o iso3_d countryName_d using "processed_data/temp/OECD/`f'.dta", clear
    stack iso3_o countryName_o iso3_d countryName_d, into(iso3 countryName) clear
    drop _stack
    duplicates drop
    tempfile ctyList_`f'
    save `ctyList_`f'', replace
}

clear
foreach f of local fileList {
    append using `ctyList_`f''
}
duplicates drop
ren countryName countryName_OECD

merge m:1 iso3 using "processed_data/isoStandard.dta"
sort _merge iso3
export excel using "processed_data/isoStandard.xlsx", sheet("OECD_input") sheetreplace firstrow(variables)

** manually adjust and export to sheet "OECD_output"
    