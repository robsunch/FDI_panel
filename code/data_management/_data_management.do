*******************************************
** master file for the folder data_management
*******************************************
clear all
set more off
set matsize 5000
set emptycells drop

cd $cdDir
global codeDir "code/data_management"
global figureDir "output/data_management/figures"
global tableDir "output/data_management/tables"
global logDir "output/data_management/log"

** create output folders
capture mkdir "output/data_management"
capture mkdir $figureDir
capture mkdir $tableDir
capture mkdir $logDir

** do "$codeDir/isoStandard.do"
** do "$codeDir/exchRate.do"    

** ** import data
** do "$codeDir/eurostat_to_dta.do"
** do "$codeDir/eurostat_varlist.do"
** do "$codeDir/eurostat_ind_agg.do"

** do "$codeDir/OECD_to_dta.do"
** do "$codeDir/OECD_varlist.do"
** do "$codeDir/OECD_ind_agg.do"

** do "$codeDir/UNCTAD_import.do"
** do "$codeDir/UNCTAD_country_codes.do"
** do "$codeDir/UNCTAD_clean.do"

** ** combine different sources
** do "$codeDir/combine_OECD_es_unctad_flow_stock.do"
** do "$codeDir/OECD_combine_isic3_isic4.do"
** do "$codeDir/combine_OECD_eurostat_activities.do"
** do "$codeDir/consolidate_emp.do"

** ** additional aggregate data
** do "$codeDir/world_klems.do" // total output data from World Klems
** do "$codeDir/stan_prod.do" // total output data from STAN
** do "$codeDir/nonfin_output_share.do" // calculate nonfin output share
** do "$codeDir/gdp.do" // construct current price GDP for each country
** do "$codeDir/agg_extrap.do" // extrapolate aggregate output and employment
** do "$codeDir/nonfin_gdp_share.do" // calculate nonfinancial share in GDP in case nonfin output share is missing

** do "$codeDir/exclude_fin.do" // exclude financial activities from OECD-Eurostat data

do "$codeDir/extrap_bilat_activities.do" // extrapolate missing values
** do "$codeDir/extrap_tot_in_activities.do" // extrapolate missing values

** remove temporary files
** !rmdir "processed_data/temp" /q /s // to delete nonempty folders need to use shell commands
    