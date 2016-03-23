*****************************************************
** this do-file exports a list of variables of OECD MNE activities
*****************************************************

log close _all
log using "$logDir/OECD_varlist.smcl", replace

local data_in = "processed_data/temp/OECD"
local outputPath = "$tableDir/OECD_varlist.xlsx"


local fileList AMNE_OUT4_world_total AMNE_OUT4_main_sectors AMNE_IN4_main_sectors
foreach f in `fileList' {
    use var var_des using "`data_in'/`f'.dta", clear
    duplicates drop
    tempfile `f'
    save ``f'', replace
}
clear
foreach f in `fileList' {
    append using ``f''
}
duplicates drop
export excel using "`outputPath'", sheet("OECD_AMNE") sheetreplace firstrow(variables)
export excel using "processed_data/select_var.xlsx", sheet("OECD_AMNE_input") sheetreplace firstrow(variables)
**** manually select variables and put in sheet "OECD_AMNE_output"


*******************************
** check variable in fats tables
*******************************
local fileList FATS_OUT3_main_sectors FATS_IN3_main_sectors
foreach f in `fileList' {
    use var var_des using "`data_in'/`f'.dta", clear
    duplicates drop
    tempfile `f'
    save ``f'', replace
}
clear
foreach f in `fileList' {
    append using ``f''
}
duplicates drop
export excel using "`outputPath'", sheet("OECD_FATS") sheetreplace firstrow(variables)
export excel using "processed_data/select_var.xlsx", sheet("OECD_FATS_input") sheetreplace firstrow(variables)
**** manually select variables and put in sheet "OECD_FATS_output"

log close _all


