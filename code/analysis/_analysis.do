*******************************************
** master file for the folder analysis
*******************************************
clear all
set more off
set matsize 1000

cd "C:/LocalProjects/FDI_panel"
global codeDir "code/analysis"
global figureDir "output/analysis/figures"
global tableDir "output/analysis/tables"
global logDir "output/analysis/log"

** create output folders
capture mkdir "output/analysis"
capture mkdir $figureDir
capture mkdir $tableDir
capture mkdir $logDir

do "$codeDir/activity_oecd_vs_eurostat.do"
