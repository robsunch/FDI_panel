***********************************
** master do-file for tables and figures in 
** paper version 1
***********************************

capture mkdir "output/writing/paper/v1"
global figureDir "output/writing/paper/v1/figures"
global tableDir "output/writing/paper/v1/tables"
global logDir "output/writing/paper/v1/log"

capture mkdir $figureDir
capture mkdir $tableDir
capture mkdir $logDir

** tables
** do "$codeDir/paper/v1/tables/oecd_overlap_years.do"
** do "$codeDir/paper/v1/tables/emp_vs_psn_emp.do"
do "$codeDir/paper/v1/tables/exclude_fin.do" 

** do "$codeDir/paper/v1/tables/nonmiss_bilat_by_year.do"

** figures
** do "$codeDir/paper/v1/figures/tot_inward_trend_HUN.do"
