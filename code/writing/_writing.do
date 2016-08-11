cd $cdDir
global codeDir "code/writing"

** create output folders
capture mkdir "output/writing"
capture mkdir "output/writing/paper"

do "$codeDir/paper/v1/_v1.do"
