*******************************************
** master file for the folder data_management
*******************************************
clear all
set more off
set matsize 1000

cd "C:/LocalProjects/FDI_panel"
global codeDir "code/data_management"

** create output folders
capture mkdir "output/data_management"
capture mkdir "output/data_management/figures"
capture mkdir "output/data_management/tables"
capture mkdir "output/data_management/log"

global figureDir "output/data_management/figures"
global tableDir "output/data_management/tables"
global logDir "output/data_management/log"

** import data
do "$codeDir/import_OECD.do"
do "$codeDir/import_UNCTAD.do"
do "$codeDir/import_eurostat.do"
/*
** basic data cleaning
do "$codeDir/clean_OECD.do"
do "$codeDir/clean_UNCTAD.do"
do "$codeDir/clean_eurostat.do"
