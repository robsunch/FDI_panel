****************************************************
** this do-file combines OECD and Eurostat inward and outward
** activities, convert monetary variables to USD
** and compare four definitions for each variable: 
** inward OECD, outward OECD, inward Eurostat, outward Eurostat
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
    
local keepList lcu_to_eur_es fixedRate year_of_adoption   
 
ren iso3_d iso3
merge m:1 iso3 year using "processed_data/exchRate.dta", keep(master match) nogen keepusing(`keepList')
** attempt to adjust es_in_rev to Euros used in OECD
gen adj_es_in_rev_totXfin = es_in_rev_totXfin * lcu_to_eur_es if year_of_adoption == .
replace adj_es_in_rev_totXfin = es_in_rev_totXfin if year_of_adoption < . & year >= year_of_adoption
replace adj_es_in_rev_totXfin = es_in_rev_totXfin * lcu_to_eur_es / fixedRate if year_of_adoption < . & year < year_of_adoption
quietly count if adj_es_in_rev_totXfin == . & es_in_rev_totXfin < .
if `r(N)' > 0 {
    disp as error "Some es_in_rev_totXfin cannot be adjusted using LCU-EUR exchange rates. Check."
    error 1 
}
ren iso3 iso3_d
drop `keepList'

ren iso3_o iso3
merge m:1 iso3 year using "processed_data/exchRate.dta", keep(master match) nogen keepusing(`keepList')
** attempt to adjust es_out_rev_tot es_out_rev_fin to Euros used in OECD
foreach x in tot fin {
    gen adj_es_out_rev_`x' = es_out_rev_`x' * lcu_to_eur_es if year_of_adoption == .
    replace adj_es_out_rev_`x' = es_out_rev_`x' if year_of_adoption < . & year >= year_of_adoption
    replace adj_es_out_rev_`x' = es_out_rev_`x' * lcu_to_eur_es / fixedRate if year_of_adoption < . & year < year_of_adoption
    quietly count if adj_es_out_rev_`x' == . & es_out_rev_`x' < .
    if `r(N)' > 0 {
        disp as error "Some es_out_rev_`x' cannot be adjusted using LCU-EUR exchange rates. Check."
        error 1 
    }
}
ren iso3 iso3_o
drop `keepList'
    
foreach x in in_rev_totXfin out_rev_tot out_rev_fin {
    gen diff_adj_`x' = (oecd_`x' - adj_es_`x') /  (oecd_`x' + adj_es_`x') * 2
}    

estpost summarize diff_adj_*, detail quietly
esttab . using "`outputPath'", append cell("mean sd min p10 p25 p50 p75 p90 max count") ///
    noobs title("% diff in OECD and Eurostat variables AFTER exchange rate adjustments")
drop diff_adj_*    
restore

***********************************
** exchange rate  adjustment for OECD and
** Eurostat datasets
************************************
foreach f in out in {
    if "`f'" == "in" {
        local direc d
    }
    else if "`f'" == "out" {
        local direc o
    }

    ren iso3_`direc' iso3
    
    local keepList exchRate_wdi lcu_to_eur_es euroExchRate fixedRate year_of_adoption
    merge m:1 iso3 year using "processed_data/exchRate.dta", keep(master match) nogen ///
        keepusing(`keepList')
    
    ** Eurostat
    quietly ds es_`f'*rev*
    foreach x in `r(varlist)' {
        if ~regexm("`x'","flag") {
            count if ~missing(`x') & year<=1998
            if `r(N)' > 0 {
                estpost tabulate iso3 year if ~missing(`x') & year<=1998
                esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Eurostat `x' before 1998") ///
                eqlabels(, lhs("iso3_`direc' \ year")) varlabels(`e(labels)')
            }
            
            gen temp = .
            replace temp = `x' * euroExchRate * 1e6 if year>=1999
            replace temp = `x' * lcu_to_eur_es / exchRate_wdi * 1e6 if year<1999 // change to LCU then use WDI exchange rates
            count if temp==. & `x'<.
            if `r(N)'==0 {
                drop `x'
                ren temp `x'
                label var `x' "Unit: USD"
            }
            else {
                display as error "Exchange rate adjustments did not cover some obs for `x'. Check."
                tab year iso3 if temp==. & `x'<.
                drop `x'
                ren temp `x'
                label var `x' "Unit: USD"
                ** error 1
            }            
        }
    }
    
    ** OECD
    quietly ds oecd_`f'*rev*
    foreach x in `r(varlist)' {
        if ~regexm("`x'","flag") {
            count if ~missing(`x') & year<=1998
            if `r(N)' > 0 {
                estpost tabulate iso3 year if ~missing(`x') & year<=1998
                esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("OECD `x' before 1998") ///
                eqlabels(, lhs("iso3_`direc' \ year")) varlabels(`e(labels)')
            }
            
            gen temp = .
            replace temp = `x' * euroExchRate * 1e6 if year>=year_of_adoption & year_of_adoption < . // EMU country years
            replace temp = `x' * fixedRate / exchRate_wdi * 1e6 if year<year_of_adoption & year_of_adoption < . // Pre-EMU
            replace temp = `x' / exchRate_wdi * 1e6 if year_of_adoption == . // non-EMU
            count if temp==. & `x'<.
            if `r(N)'==0 {
                drop `x'
                ren temp `x'
                label var `x' "Unit: USD"
            }
            else {
                display as error "Exchange rate adjustments did not cover some obs for `x'. Check."
                tab year iso3 if temp==. & `x'<.
                drop `x'
                ren temp `x'
                label var `x' "Unit: USD"
                ** error 1
            }            
        }
    }
    
    ren iso3 iso3_`direc'
    drop `keepList'
}

*** adjust for Germany's monetary values in 2007 in OECD outward
egen id_pair = group(iso3_o iso3_d)
xtset id_pair year
ds oecd_out*, has(type numeric)
foreach x in `r(varlist)' {
    gen gr_`x' = `x' / l.`x' - 1
}
estpost tabstat gr_* if iso3_o == "DEU", by(year) statistics(mean count) ///
    columns(statistics) quietly
esttab . using "`outputPath'", append main(mean) aux(count) nogap nostar ///
    unstack noobs nonote label ///
    title("mean (N) growth rates - Germany - before 2007 adjustment")


ds oecd_out*rev*, has(type numeric)
foreach x in `r(varlist)' {
    replace `x' = `x' / 1000 if year == 2007 & iso3_o == "DEU"
    replace gr_`x' = `x' / l.`x' - 1
}
estpost tabstat gr_* if iso3_o == "DEU", by(year) statistics(mean count) ///
    columns(statistics) quietly
esttab . using "`outputPath'", append main(mean) aux(count) nogap nostar ///
    unstack noobs nonote label ///
    title("mean (N) growth rates - Germany - after 2007 adjustment")       
drop gr_* id_pair
  
compress
sort iso3_o iso3_d year
save "processed_data/activity_OECD_eurostat_combined.dta", replace


log close _all
