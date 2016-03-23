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

log close _all
log using "$logDir/activity_oecd_vs_eurostat.smcl", replace

local outputPath = "$tableDir/activity_oecd_vs_eurostat.csv"
capture rm "`outputPath'"

*** EUR to USD exchange rate
import excel date euroExchRate using "source_data/misc/DEXUSEU.xls", clear cellrange(a15:b30)
gen year = year(date)
tempfile euroExchRate
save `euroExchRate', replace

*** Euro fixed rates
import excel using "source_data/misc/euroFixedRate.xlsx", clear firstrow
gen year_of_adoption = year(date_of_adoption)
tempfile euroFixedRates
save `euroFixedRates', replace

*** World Bank official exchange rates
import excel using "source_data/misc/pa.nus.fcrf_Indicator_en_excel_v2.xls", sheet("Data") cellrange(a3:bf261) firstrow clear
drop Indicator* CountryName
ren CountryCode iso3
quietly ds iso3, not
foreach x of varlist `r(varlist)' {
	local xlab : var label `x'
	ren `x' year`xlab'
	}
reshape long year, i(iso3) j(yr)
ren year exchRate_wdi
ren yr year

merge m:1 iso3 using `euroFixedRates', nogen
merge m:1 year using `euroExchRate', keep(master match) nogen

gen exchRate = exchRate_wdi
*** adjust for euro fixed rates before adoption
replace exchRate = exchRate / fixedRate if year<year_of_adoption & year_of_adoption<=2013
*** adjust for euro exchange rates after adoption
replace exchRate = 1/euroExchRate if year>=year_of_adoption & year_of_adoption<=2013 & missing(exchRate)

keep iso3 year year_of_adoption exchRate exchRate_wdi fixedRate
label var fixedRate "Euro fixed rate"
label var exchRate "Exchange rate LCU (converted Euro for EMU): 1 USD"
label var exchRate_wdi "WDI exchange rate LCU : 1 USD"
tempfile exchRate
save `exchRate', replace

*** check whether Eurostat reporting countries are in EMU
use if report_es_in | report_es_out using "processed_data/activity_reporting_OECD_eurostat.dta", clear
merge 1:1 iso3 using `euroFixedRates'

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
        replace iso2 = "WD" if iso2 == "WORLD"
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
        replace iso3 = "WRD" if iso2=="WD"
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

foreach f in out in {
    use "processed_data/OECD/activity_`f'.dta", clear
    foreach x in o d {
        ren iso3_`x' iso3_OECD
        replace iso3_OECD = "WRD" if iso3_OECD == "WORLD"
        merge m:1 iso3_OECD using `OECD_standard_ctyCode', keepusing(iso3) keep(master match)   
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
            capture estpost tabulate countryName_`x' if iso3_OECD=="WRD"
            capture esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Country Name for iso3 code WRD - WORLD") ///
                varlabels(`e(labels)')
        }
        drop if _merge==1
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

*** OECD data is in millions of LCU while ES data is in millions of EUR
*** convert both to USD
foreach f in out in {
    if "`f'" == "in" {
        local direc d
    }
    else if "`f'" == "out" {
        local direc o
    }

    ren iso3_`direc' iso3
    
    merge m:1 iso3 year using `exchRate', keep(master match) nogen keepusing(exchRate)
    quietly ds *oecd_`f'*rev*
    foreach x in `r(varlist)' {
        if ~regexm("`x'","flag") {
            replace `x' = `x' / exchRate * 1e6 // mil LCU to 1 USD
        }
    }
    merge m:1 year using `euroExchRate', keep(master match) keepusing(euroExchRate) nogen
    quietly ds *es_`f'*rev*
    foreach x in `r(varlist)' {
        if ~regexm("`x'","flag") {
            count if ~missing(`x') & year<=1998
            if `r(N)' > 0 {
                estpost tabulate iso3 year if ~missing(`x') & year<=1998
                esttab . using "`outputPath'", append cell(b) unstack noobs nonumber nomtitle ///
                title("Eurostat `x' before 1998") ///
                eqlabels(, lhs("iso3_`direc' \ year")) varlabels(`e(labels)')
            }
            replace `x' = `x' / euroExchRate * 1e6 // mil Euro to 1 USD. Obs before 1998 become missing
        }
    }
    
    ren iso3 iso3_`direc'
    drop exchRate euroExchRate
}


compress
save "processed_data/activity_OECD_eurostat_combined.dta", replace


log close _all
