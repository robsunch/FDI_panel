*******************************************
** master file for the folder analysis
*******************************************
cd $cdDir
global codeDir "code/analysis"
global figureDir "output/analysis/figures"
global tableDir "output/analysis/tables"
global logDir "output/analysis/log"

** create output folders
capture mkdir "output/analysis"
capture mkdir $figureDir
capture mkdir $tableDir
capture mkdir $logDir

do "$codeDir/total_activity_trends.do"
