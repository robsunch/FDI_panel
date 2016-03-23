******************************************************************
** this do-file imports OECD bilateral MNE activity data
** basic renaming and save in processed_data/temp
**
*******************************************************************

log close _all
log using "$logDir/OECD_to_dta.smcl", replace

capture mkdir "processed_data/temp"
capture mkdir "processed_data/temp/OECD"

local data_out = "processed_data/temp/OECD"

** outward activity
foreach f in AMNE_OUT4_world_total AMNE_OUT4_main_sectors {
    insheet using "source_data/OECD/`f'.csv", names clear
    ren (cou declaringcountry part partnercountry economicvariable industry flagcodes flags) ///
        (iso3_o countryName_o iso3_d countryName_d var_des ind_des flag flag_des)
    keep iso3_* countryName_* var* ind* year value flag*
    save "`data_out'/`f'.dta", replace
}

insheet using "source_data/OECD/FATS_OUT3_main_sectors.csv", names clear
ren (cou country part partnercountries variables serv services flagcodes flags) ///
    (iso3_o countryName_o iso3_d countryName_d var_des ind ind_des flag flag_des)
keep iso3_* countryName_* var* ind* year value flag*
save "`data_out'/FATS_OUT3_main_sectors.dta", replace

** inward activity
insheet using "source_data/OECD/AMNE_IN4_main_sectors.csv", names clear
ren (part partnercountry cou declaringcountry economicvariable industry flagcodes flags) ///
    (iso3_o countryName_o iso3_d countryName_d var_des ind_des flag flag_des)
keep iso3_* countryName_* var* ind* year value flag*
save "`data_out'/AMNE_IN4_main_sectors.dta", replace

insheet using "source_data/OECD/FATS_IN3_main_sectors.csv", names clear
ren (part partnercountries cou country variables serv services flagcodes flags) ///
    (iso3_o countryName_o iso3_d countryName_d var_des ind ind_des flag flag_des)
keep iso3_* countryName_* var* ind* year value flag*
save "`data_out'/FATS_IN3_main_sectors.dta", replace




