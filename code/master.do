****************************************
** master file to run everything from scratch
****************************************
clear all

** global cdDir "C:/LocalProjects/FDI_panel"
global cdDir "C:/Users/robsunch/Dropbox/Projects/FDI_panel"
cd $cdDir

/*
!rmdir output /q /s
!mkdir output
*/

** do "code/data_management/_data_management.do"
** do "code/check_data/_check_data.do"
do "code/analysis/_analysis.do" 
