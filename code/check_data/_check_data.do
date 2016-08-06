*******************************************
** master file for the folder check_data
*******************************************
clear all
set more off
set matsize 1000

cd $cdDir
global codeDir "code/check_data"
global figureDir "output/check_data/figures"
global tableDir "output/check_data/tables"
global logDir "output/check_data/log"

** create output folders
capture mkdir "output/check_data"
capture mkdir $figureDir
capture mkdir $tableDir
capture mkdir $logDir

do "$codeDir/total_output.do"
do "$codeDir/emp_vs_psn_emp.do"
do "$codeDir/activity_oecd_vs_eurostat.do"
do "$codeDir/eurostat_bop_vs_tec.do"
do "$codeDir/flow_stock_oecd_vs_eurostat_vs_unctad.do"

do "$codeDir/activity_inward_vs_outward.do"

** do "$codeDir/fully_connected.do"
