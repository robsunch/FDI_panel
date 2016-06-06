*********************************
** compare FDI flow and stocks from 3 
** sources: OECD, Eurostat and UNCTAD
*********************************

log close _all
log using "$logDir/flow_stock_oecd_vs_eurostat_vs_unctad.smcl", replace

local outputPath = "$tableDir/flow_stock_oecd_vs_eurostat_vs_unctad.csv"
capture rm "`outputPath'"

use "processed_data/OECD_ES_UNCTAD_flow_stock.dta", clear

** any info for host*home*year
drop *_notes *_source // drop strings
foreach s in UNCTAD oecd es {
    egen anyInfo_`s' = rownonmiss(`s'_*)
    replace anyInfo_`s' = anyInfo_`s' > 0
}
estpost tabstat anyInfo_*, statistics(sum) by(year) quietly elabels
esttab . using "`outputPath'", append main(sum) unstack noobs nonote ///
    title("any nonmissing values from the corresponding sources") ///
    cells("anyInfo_UNCTAD anyInfo_oecd anyInfo_es")
drop anyInfo_*

** check coverage and discrepancy of each variable
foreach direc in in out {
foreach x in stock flow {
    foreach s in UNCTAD oecd es {
        gen des_`s' = "miss" if missing(`s'_`direc'_`x')
        replace des_`s' = ">0" if `s'_`direc'_`x'>0 & `s'_`direc'_`x'<.
        replace des_`s' = "=0" if `s'_`direc'_`x'==0
        replace des_`s' = "<0" if `s'_`direc'_`x'<0
    }
    estpost tabulate des_UNCTAD des_es, elabels quietly 
    esttab . using "`outputPath'", append unstack noobs nonote ///
        title("Coverage of `direc' `x'") ///
        eqlabels(, lhs("UNCTAD \ eurostat")) varlabels(`e(labels)') 
    estpost tabulate des_oecd des_es, elabels quietly 
    esttab . using "`outputPath'", append unstack noobs nonote ///
        title("Coverage of `direc' `x'") ///
        eqlabels(, lhs("OECD \ eurostat")) varlabels(`e(labels)')         
    
    gen diff_UNCTAD_es_`direc'_`x' = UNCTAD_`direc'_`x' / es_`direc'_`x' - 1 if des_UNCTAD==">0" & des_es==">0"
    gen diff_oecd_es_`direc'_`x' = oecd_`direc'_`x' / es_`direc'_`x' - 1 if des_oecd==">0" & des_es==">0"
    gen diff_UNCTAD_oecd_`direc'_`x' = UNCTAD_`direc'_`x' / oecd_`direc'_`x' - 1 if des_UNCTAD==">0" & des_oecd==">0"
    drop des_*
    
    estpost summarize diff_*, quietly detail
    esttab . using "`outputPath'", append cells("mean sd count min p1 p5 p10 p25 p50 p75 p90 p95 p99 max") noobs ///
    title("Summary stats for difference of `direc' `x' from different sources")
    drop diff_*

}
}

** compare inward and outward stocks

log close _all
