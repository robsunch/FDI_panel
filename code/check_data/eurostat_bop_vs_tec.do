**************************************
** this do-file compares FDI flow/stock data from 
** BOP and TEC tables in Eurostat
**************************************

local outputPath = "$tableDir/eurostat_bop_vs_tec.csv"
capture rm "`outputPath'"

use if inrange(year,2008,2009) using "processed_data/eurostat/bop_stock_flow.dta", clear
ren eurostat_* bop_*

**** compare NACE R1 v.s. NACE R2 data ****
foreach direc in in out {
foreach x in stock flow {
    foreach v in r1 r2 {
        gen des_`v' = "miss" if missing(bop_`direc'_`x'_`v')
        replace des_`v' = "<0" if bop_`direc'_`x'_`v'<0
        replace des_`v' = "=0" if bop_`direc'_`x'_`v'==0
        replace des_`v' = ">0" if bop_`direc'_`x'_`v'>0 & bop_`direc'_`x'_`v'<.
    }
    estpost tabulate des_r1 des_r2
    esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Compare bop_`direc'_`x'_r1 and bop_`direc'_`x'_r2") ///
            eqlabels(, lhs("r1 \ r2"))
    drop des_*
    
    gen diff_`direc'_`x' = bop_`direc'_`x'_r2 / bop_`direc'_`x'_r1 - 1
    
}
}     
   
estpost summarize diff_*, detail
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("difference between r2 and r1")

use using "processed_data/eurostat/bop_stock_flow.dta", clear
ren eurostat_* bop_*    
*** for simplicity here, use BOP R2 after 2008, use BOP R1 before 2007    
foreach direc in in out {
foreach x in stock flow {
    gen bop_`direc'_`x' = bop_`direc'_`x'_r2 if year>=2008
    replace bop_`direc'_`x' = bop_`direc'_`x'_r1 if year < 2008
}
} 

merge 1:1 iso2_o iso2_d year using "processed_data/eurostat/tec_stock_flow.dta"
estpost tabulate year _merge, elabels
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Data in BOP (master) and TEC (using) respectively") ///
            eqlabels(, lhs("year \ merge")) varlabels(`e(labels)')

ren eurostat_* tec_*
foreach direc in in out {
foreach x in stock flow {
    foreach s in bop tec {
        gen des_`s' = "miss" if missing(`s'_`direc'_`x')
        replace des_`s' = "<0" if `s'_`direc'_`x'<0
        replace des_`s' = "=0" if `s'_`direc'_`x'==0
        replace des_`s' = ">0" if `s'_`direc'_`x'>0 & `s'_`direc'_`x'<.
    }
    estpost tabulate des_bop des_tec
    esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Compare bop_`direc'_`x' and tec_`direc'_`x'") ///
            eqlabels(, lhs("bop \ tec"))
    drop des_*    
    gen diff_`direc'_`x' = tec_`direc'_`x' / bop_`direc'_`x' - 1
}
} 

estpost summarize diff_*, detail
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("difference between bop and tec")



            
