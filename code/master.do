****************************************
** master file to run everything from scratch
****************************************
clear all

cd "C:/LocalProjects/FDI_panel"
!rmdir output /q /s

!mkdir output

do "code/data_management/_data_management.do"
do "code/check_data/_check_data.do"
do "code/analysis/_analysis.do" 
