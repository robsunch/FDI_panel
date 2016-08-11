echo *************************************************
echo * this script copies output to the paper folder
echo *************************************************

set source_dir=C:\Users\robsunch\Dropbox\Projects\FDI_panel\output\writing\paper\v1
set desti_dir=C:\Users\robsunch\Dropbox\Projects\FDI_panel\writing\paper\v1
copy %source_dir%\figures\*.eps %desti_dir%\figures
copy %source_dir%\figures\*.pdf %desti_dir%\figures
copy %source_dir%\tables\*.tex %desti_dir%\tables
REM pause
