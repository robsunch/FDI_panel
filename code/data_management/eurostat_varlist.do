*****************************************************
** this do-file exports a list of eurostat MNE activities
*****************************************************

log close _all
log using "$logDir/eurostat_varlist.smcl", replace

local data_in = "processed_data/temp/eurostat"
local outputPath = "$tableDir/eurostat_varlist.xlsx"

****************
*** dictionary files
****************
foreach x in geo indic_sb indic_bp nace_r2 nace_r1 {
	insheet `x' `x'_des using "source_data/eurostat/dic/`x'.dic", tab clear
	tempfile `x'
	save ``x'', replace
}

*******************************
** check variable codes in fats tables
*******************************
foreach x in fats_g1b_08 fats_g1b_03 {
    use indic_sb using "`data_in'/`x'.dta", clear
    duplicates drop
    merge 1:1 indic_sb using `indic_sb', keep(master match) nogen
    export excel indic_sb indic_sb_des using "`outputPath'", sheet("`x'") sheetreplace firstrow(variables)
    tempfile var_`x'
    save `var_`x'', replace
}

*** fats 96 tables
local fileList : dir "`data_in'/fats_96/" files "fats_*.dta"
tempfile to_add
clear
foreach f in `fileList' {
    preserve
    use indic_sb using "`data_in'/fats_96/`f'", clear
    duplicates drop
    save `to_add', replace
    restore
    append using `to_add'  
}    
duplicates drop
merge 1:1 indic_sb using `indic_sb', keep(master match) nogen
export excel indic_sb indic_sb_des using "`outputPath'", sheet("fats_96") sheetreplace firstrow(variables)
tempfile var_fats_96
save `var_fats_96', replace

clear
foreach x in fats_g1b_08 fats_g1b_03 fats_96 {
    append using `var_`x''
}
duplicates drop
export excel indic_sb indic_sb_des using "processed_data/select_var.xlsx", sheet("eurostat_indic_sb_input") sheetreplace firstrow(variables)
**** manually select variables and put in sheet "eurostat_indic_sb_output"


*** fats out tables
foreach x in fats_out1 fats_out2 fats_out2_r2 {
    use indic_bp using "`data_in'/`x'.dta", clear
    duplicates drop
    merge 1:1 indic_bp using `indic_bp', keep(master match) nogen
    export excel indic_bp indic_bp_des using "`outputPath'", sheet("`x'") sheetreplace firstrow(variables)
    tempfile `x'
    save ``x'', replace
}

clear
foreach x in fats_out1 fats_out2 fats_out2_r2 {
    append using ``x''
}
duplicates drop
export excel indic_bp indic_bp_des using "processed_data/select_var.xlsx", sheet("eurostat_indic_bp_input") sheetreplace firstrow(variables)
**** manually select variables and put in sheet "eurostat_indic_bp_output"

log close _all


