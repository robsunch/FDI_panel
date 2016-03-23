*******************************************
** master file for the folder data_management
*******************************************
clear all
set more off
set matsize 1000

cd "C:/LocalProjects/FDI_panel"
global codeDir "code/data_management"
global figureDir "output/data_management/figures"
global tableDir "output/data_management/tables"
global logDir "output/data_management/log"

** create output folders
capture mkdir "output/data_management"
capture mkdir $figureDir
capture mkdir $tableDir
capture mkdir $logDir
    
do "$codeDir/isoStandard.do"
    
** import data
do "$codeDir/eurostat_to_dta.do"
do "$codeDir/eurostat_varlist.do"
do "$codeDir/eurostat_ind_agg.do"

do "$codeDir/OECD_to_dta.do"
do "$codeDir/OECD_varlist.do"
do "$codeDir/OECD_ind_agg.do"
do "$codeDir/OECD_combine_isic3_isic4.do"

do "$codeDir/combine_OECD_eurostat_activities.do"

** remove temporary files
!rmdir "processed_data/temp" /q /s // to delete nonempty folders need to use shell commands