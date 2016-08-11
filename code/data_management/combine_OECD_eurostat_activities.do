****************************************************
** this do-file combines OECD and Eurostat inward and outward
** activities
** n_emp and n_psn_emp are consolidated
****************************************************

log close _all
log using "$logDir/combine_OECD_eurostat_activities.smcl", replace

local outputPath = "$tableDir/combine_OECD_eurostat_activities.csv"
capture rm "`outputPath'"

*** dictionary files for country name (eurostat)
foreach x in geo {
    insheet `x' `x'_des using "source_data/eurostat/dic/`x'.dic", tab clear
    ren (geo geo_des) (iso2 countryName)
    tempfile `x'_dic
    save ``x'_dic', replace
}

*** standardized country code for OECD
import excel using "processed_data/isoStandard.xlsx", sheet("OECD_output") clear firstrow
drop if missing(_merge) // drop notes
replace iso3 = iso3_OECD if missing(iso3)
keep iso3 iso3_OECD
duplicates drop
tempfile OECD_standard_ctyCode
save `OECD_standard_ctyCode', replace

*** eurostat data
foreach f in out in {
    use "processed_data/eurostat/fats_`f'.dta", clear
    if "`f'" == "out" {
        ren (geo partner) (iso2_o iso2_d)        
    }
    else if "`f'" == "in" {
        ren (geo c_ctrl) (iso2_d iso2_o)
    }
    foreach x in o d {
        ren iso2_`x' iso2
        replace iso2 = "GB" if iso2 == "UK"
        replace iso2 = "GR" if iso2 == "EL"
        replace iso2 = "CN" if iso2 == "CN_X_HK"
        if "`f'" == "in" {
            replace iso2 = "WX" if iso2 == "WRL_X_REP" // World except reporting country
            replace iso2 = "WT" if iso2 == "WORLD" // National Total
        }
        else if "`f'" == "out" {
            replace iso2 = "WX" if iso2 == "WORLD" // World except reporting country
        }
        merge m:1 iso2 using "processed_data/isoStandard.dta", keepusing(iso3) keep(master match) nogen
        merge m:1 iso2 using `geo_dic', keep(master match) nogen
        count if missing(iso3)
        if `r(N)' > 0 {    
            estpost tabulate iso2 if missing(iso3), missing sort
            esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Cases of non-standard iso2_`x' in fats_`f' by eurostat") ///
                eqlabels(, lhs("year \ merge")) varlabels(`e(labels)')
            estpost tabulate countryName if missing(iso3), missing sort
            esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Cases of non-standard iso2_`x' in fats_`f' by eurostat") ///
                eqlabels(, lhs("year \ merge")) varlabels(`e(labels)')
        }
        replace iso3 = "WRX" if iso2=="WX"
        replace iso3 = "WRT" if iso2=="WT"
        drop countryName
        drop if missing(iso3)
        ren (iso2 iso3) (iso2_`x' iso3_`x')
    }

    drop iso2*
    ds iso* year, not
    foreach x in `r(varlist)' {
        ren `x' es_`f'_`x'
    }
    
    tempfile es_`f'_to_merge
    save `es_`f'_to_merge', replace
    
    if "`f'" == "out" {
        keep iso3_o
        duplicates drop
        ren iso3_o iso3
        tempfile report_es_`f'
        save `report_es_`f'', replace
    }
    else if "`f'" == "in" {
        keep iso3_d 
        duplicates drop
        ren iso3_d iso3
        tempfile report_es_`f'
        save `report_es_`f'', replace
    }
}

*** OECD data
foreach f in out in {
    use "processed_data/OECD/activity_`f'.dta", clear
    foreach x in o d {
        ren iso3_`x' iso3_OECD
        replace iso3_OECD = "WRX" if iso3_OECD == "WORLD" // World except the reporting country
        replace iso3_OECD = "WRT" if iso3_OECD == "A1"  // National Total
        merge m:1 iso3_OECD using `OECD_standard_ctyCode', keepusing(iso3) keep(master match)   
        replace iso3 = "WRX" if iso3_OECD == "WRX"
        replace iso3 = "WRT" if iso3_OECD == "WRT"
        count if _merge==1
        if `r(N)' > 0 {
            estpost tabulate iso3_OECD if _merge==1, missing sort
            esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Cases of non-standard iso3_`x' in activity_`f' by OECD") ///
                eqlabels(, lhs("year \ merge")) varlabels(`e(labels)')
            estpost tabulate countryName_`x' if _merge==1, missing sort
            esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Cases of non-standard iso3_`x' in activity_`f' by OECD") ///
                eqlabels(, lhs("year \ merge")) varlabels(`e(labels)')
            capture estpost tabulate countryName_`x' if iso3_OECD=="WRX"
            capture esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Country Name for iso3 code WRD - WORLD") ///
                varlabels(`e(labels)')
        }
        drop if _merge==1 & ~inlist(iso3_OECD,"WRX","WRT")
        drop _merge iso3_OECD
        ren iso3 iso3_`x'
    } 
    
    drop countryName*
    ds iso* year, not
    foreach x in `r(varlist)' {
        ren `x' oecd_`f'_`x'
    }
    
    tempfile oecd_`f'_to_merge
    save `oecd_`f'_to_merge', replace
    
    if "`f'" == "out" {
        keep iso3_o
        duplicates drop
        ren iso3_o iso3
        tempfile report_oecd_`f'
        save `report_oecd_`f'', replace
    }
    else if "`f'" == "in" {
        keep iso3_d 
        duplicates drop
        ren iso3_d iso3
        tempfile report_oecd_`f'
        save `report_oecd_`f'', replace
    }
    
}

clear
foreach x1 in oecd es {
foreach x2 in in out {
    append using `report_`x1'_`x2''
}
}
duplicates drop
foreach x1 in oecd es {
foreach x2 in in out {
    merge 1:1 iso3 using `report_`x1'_`x2''
    gen report_`x1'_`x2' = _merge==3
    drop _merge
}
}
save "processed_data/activity_reporting_OECD_eurostat.dta", replace



clear
use `es_out_to_merge', clear
merge 1:1 iso3_o iso3_d year using `es_in_to_merge', nogen
merge 1:1 iso3_o iso3_d year using `oecd_out_to_merge', nogen
merge 1:1 iso3_o iso3_d year using `oecd_in_to_merge', nogen

***********************************
** check difference between OECD and 
** Eurostat monetary variables before 
** exchange rates adjustment
/*
out: 
    es: rev_tot, rev_fin
    oecd: rev_tot, rev_fin
    in common: rev_tot, rev_fin

in: 
    es: inv_tangi_totXfin, prod_v_totXfin, psn_cost_totXfin, purchase_totXfin, rev_totXfin, vadd_totXfin
    oecd: rev_totXfin, rev_fin, rev_tot
    in common: rev_totXfin
*/    
***********************************
preserve
foreach x in in_rev_totXfin out_rev_fin out_rev_tot {
    gen diff_`x' = (oecd_`x' - es_`x') / (oecd_`x' + es_`x') * 2
}
estpost summarize diff_in_rev_totXfin diff_out_rev_*, detail quietly
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("% diff in OECD and Eurostat variables BEFORE exchange rate adjustments")
restore

** cases where home country = host country
estpost tabulate iso3_o if iso3_o==iso3_d, elabels missing
esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
    title("Cases where home country is the same as host country")
drop if iso3_o==iso3_d
  
compress
sort iso3_o iso3_d year
save "processed_data/activity_OECD_eurostat_combined.dta", replace

log close _all
