**********************************************
** this do file extrapolates bilateral MNE activities
** revenue and employment
** following the procedure described in the documentation
**********************************************

local csvPath = "$tableDir/extrap_bilat_in_rev.csv"
capture rm "`csvPath'"

local xlsxPath = "$tableDir/potential_outliers.xlsx"
capture rm "`xlsxPath'"

local outlier_thresh = 5
   
program drop _all
program countCountry
    egen tag_home = tag(iso3_o) if e(sample)
    egen tag_host = tag(iso3_d) if e(sample)
    quietly sum tag_home
    estadd scalar num_home = r(sum)
    quietly sum tag_host
    estadd scalar num_host = r(sum)
    drop tag_home tag_host
end

*********************************
** combine stock information
*********************************
use "processed_data/OECD_ES_UNCTAD_flow_stock.dta", clear
foreach direc in in out {
    
    ** compare differences between oecd and ES stock
    gen diff_log_`direc'_stock = log(es_`direc'_stock) - ///
        log(oecd_`direc'_stock)
    
    gen `direc'_stock = es_`direc'_stock // use ES as primary source
    gen source_`direc'_stock = "es" if es_`direc'_stock<.
    replace source_`direc'_stock = "oecd" if oecd_`direc'_stock<. & `direc'_stock==.
    replace `direc'_stock = oecd_`direc'_stock if source_`direc'_stock == "oecd"
    estpost tabulate source_`direc'_stock, missing
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle ///
        title("Sources for `direc' stock") varlabels(`e(labels)')      
}

estpost summarize diff_log_*, detail quietly
esttab . using "`csvPath'", append cell("mean sd min p1 p5 p10 p25 p50 p75 p90 p95 p99 max count") ///
    noobs title("summary of diff log points")

keep iso3_o iso3_d year in_stock out_stock
tempfile stock_combined
save `stock_combined', replace


use "processed_data/nonfin_OECD_eurostat_activity.dta", clear

***********************************
** drop time series anomalies ***
***********************************
ren (*_rev_totXfin *n_emp_totXfin *n_ent_totXfin) (*_rev *n_emp *n_ent) // shorten variable names
sort iso3_o iso3_d year
egen id_pair = group(iso3_o iso3_d)
xtset id_pair year
foreach x in n_emp rev n_ent { 
foreach s in oecd es {
foreach direc in in out {
    egen temp = mean(log(`s'_`direc'_`x')), by(id_pair)
    gen dev_avg = log(`s'_`direc'_`x') - temp
    gen tag = (dev_avg>`outlier_thresh' & dev_avg<.) | dev_avg<-`outlier_thresh'
    egen tot_tag = total(tag), by(id_pair)
    gen gr_log = dev_avg - l.dev_avg
    gen f_gr_log = f.dev_avg - dev_avg
    
    gen outlier_condi_1 = (dev_avg > `outlier_thresh' & dev_avg<.) | dev_avg < - `outlier_thresh'
    gen outlier_condi_2 = (gr_log > `outlier_thresh'| gr_log<.) | (gr_log < - `outlier_thresh') | ///
                          (f_gr_log>`outlier_thresh'|f_gr_log<.) | (f_gr_log<-`outlier_thresh')
    
    gen tag_outlier_`s'_`direc'_`x' = outlier_condi_1 & outlier_condi_2
    
    count if tot_tag > 0
    if `r(N)' > 0 {
        export excel iso3_o iso3_d year *`s'_`direc'_`x' dev_avg gr_log if tot_tag > 0 using "`xlsxPath'", ///
            sheetreplace sheet("`s'_`direc'_`x'") firstrow(variables)
    }        
    
    ** replace outlier as zero
    drop tag tot_tag dev_avg temp gr_log f_gr_log outlier_condi_*
    
}
}
}

foreach x in n_emp rev n_ent { 
foreach s in oecd es {
foreach direc in in out {
    replace `s'_`direc'_`x' = . if tag_outlier_`s'_`direc'_`x'
}
}
}
*** summary stats
estpost summarize tag_outlier_*, quietly 
esttab . using "`csvPath'", append cells("sum count") noobs ///
    title("Number of outliers")
    
drop id_pair tag_*
    
*********************************
** supplement missing country in Eurostat
** with OECD
*********************************
foreach direc in in out {
    if "`direc'" == "in" {
        local suf = "d"
    }
    else {
        local suf = "o"
    }
    
    ren iso3_`suf' iso3 
    merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", nogen
    ren iso3 iso3_`suf'
    
    foreach x in rev n_emp n_ent {
        gen `direc'_`x' = es_`direc'_`x' if report_es_`direc'==1
        replace `direc'_`x' = oecd_`direc'_`x' if report_oecd_`direc'==1 & ///
            report_es_`direc'~=1
    }
    
    foreach x in rev n_emp n_ent {
        gen impute_`direc'_`x' = 0 if `direc'_`x'<.
    }
    
}

    
*********************************
** filling zeros: 
** step 1 : impute zeros using the other source
** step 2: impute zeros using FDI stocks 
** step 3: impute zero revenue using employment or # of enterprises
*********************************

foreach direc in in out {
    if "`direc'" == "in" {
        local suf = "d"
    }
    else {
        local suf = "o"
    }
    
    merge 1:1 iso3_o iso3_d year using `stock_combined', ///
        keep(master match) nogen
  
    preserve
    foreach x in rev n_emp n_ent {

        ** step 1
        replace impute_`direc'_`x' = 10 if report_es_`direc'==1 & ///
            `direc'_`x'==. & oecd_`direc'_`x' == 0 
        replace `direc'_`x' = 0 if impute_`direc'_`x' == 10
        
        replace impute_`direc'_`x' = 11 if report_es_`direc'==1 & ///
            `direc'_`x'==. & oecd_`direc'_`x' > 0 & oecd_`direc'_`x'<.
        replace `direc'_`x' = oecd_`direc'_`x' if impute_`direc'_`x' == 11

        ** step 2
        replace impute_`direc'_`x' = 20 ///
            if `direc'_stock<=0 & `direc'_`x'==.
        replace `direc'_`x' = 0 if impute_`direc'_`x' == 20       
        
    }
    
    ** step 3
    replace impute_`direc'_rev = 30 ///
        if `direc'_rev == . & ///
           ( (`direc'_n_emp == 0 & `direc'_n_ent==.) | ///
           (`direc'_n_emp == . & `direc'_n_ent==0) | ///
           (`direc'_n_emp ==0 & `direc'_n_ent==0) )
           
    replace `direc'_rev = 0 if impute_`direc'_rev == 30
    
    keep iso3_o iso3_d year `direc'_n_emp `direc'_rev `direc'_n_ent `direc'_stock impute_*
    tempfile `direc'_step3
    save ``direc'_step3', replace
    restore
}

foreach direc in in out {    
    use ``direc'_step3', clear
    foreach x in rev n_emp n_ent {
        estpost tabulate impute_`direc'_`x', elabels
        esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle ///
            title("Impute cases after Step 3 : `direc' `x'") varlabels(`e(labels)')
    }
}

use `in_step3', clear
merge 1:1 iso3_o iso3_d year using `out_step3', nogen
foreach direc in in out {
    if "`direc'" == "in" {
        local reverse_direc = "out"
    }
    else {
        local reverse_direc = "in"
    }
    
    *** step 4: impute additional zero revenue using opposite direction
    gen temp1 = `reverse_direc'_rev<=0 | `reverse_direc'_n_ent==0 | `reverse_direc'_n_emp==0
    gen temp2 = (`reverse_direc'_rev>0 & `reverse_direc'_rev<.) | ///
                (`reverse_direc'_n_ent>0 & `reverse_direc'_n_ent<.) | ///
                (`reverse_direc'_n_emp>0 & `reverse_direc'_n_emp<.)
    replace impute_`direc'_rev = 40 ///
        if `direc'_rev==. & temp1 & ~temp2 & `direc'_stock==.
    replace `direc'_rev = 0 if impute_`direc'_rev == 40
    drop temp*
    foreach x in rev n_emp n_ent {
        ren `direc'_`x' `direc'_`x'_step4
    }
}

capture label drop lab_impute_steps
do "$codeDir/lab_impute_steps.do"
label values impute_* lab_impute_steps

compress
save "processed_data/extrap_bilat_activities.dta", replace

*******************************************
** step 5 : cross-section regression extrapolation
*******************************************

** define OECD-ES sample
foreach suf in o d {
    ren iso3_`suf' iso3
    merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", ///
        keepusing(iso3) gen(report_`suf')
    gen temp = 1 if report_`suf'==3
    replace temp = 0 if report_`suf' == 1
    drop report_`suf'
    ren (iso3 temp) (iso3_`suf' report_`suf')
}    

local direc in
gen log_in_rev = log(in_rev_step4) if report_o==1 & report_d==1
gen log_out_rev = log(out_rev_step4) if report_o==1 & report_d==1
gen log_in_stock = log(in_stock) if report_o==1 & report_d==1

encode iso3_o, gen(id_iso3_o)
encode iso3_d, gen(id_iso3_d)
egen id_pair = group(iso3_o iso3_d)

** try regression by different year range
eststo clear
foreach beginYr in 1996 2002 2008 {
foreach x in out_rev in_stock {
    local endYr = min(`beginYr' + 5 , 2012)
    foreach suf in o d {
        egen num_nonmiss_`suf' = total(year<=`endYr' & year>=`beginYr' ///
            & log_in_rev<. & log_`x'<.), by(iso3_`suf')
        count if num_nonmiss_`suf' <3 & report_`suf'==1
        if `r(N)' > 0 {
            estpost tabulate iso3_`suf' if num_nonmiss_`suf'<3 & report_`suf'==1, elabels
            esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("Country_`suf' with less than 3 observations from `beginYr' to `endYr' - in_rev and `x'") ///
                varlabels(`e(labels)')
        }
    }

    quietly eststo : reg log_in_rev log_`x' i.id_iso3_o i.id_iso3_d ///
        i.id_iso3_o#c.year i.id_iso3_d#c.year if inrange(year,`beginYr',`endYr') ///
        & num_nonmiss_o>=3 & num_nonmiss_d>=3, ro
    countCountry
    estadd local yr_range "`beginYr'-`endYr'"
    drop num_nonmiss_*
}
}

esttab * using "`csvPath'", append se r2 nogaps drop(*.id_*) ///
    scalars("num_home Num of home countries" "num_host Num of host countries" "yr_range year range" ) ///
    title("Estimate cross-sectional extrapolation for `direc' revenue every 6 years")
    
** pick one year range    
local beginYr = 1995
local endYr = 2012
eststo clear
foreach x in out_rev in_stock {
    foreach suf in o d {
        egen num_nonmiss_`suf' = total(year<=`endYr' & year>=`beginYr' ///
            & log_in_rev<. & log_`x'<.), by(iso3_`suf')
        count if num_nonmiss_`suf'<3 & report_`suf'==1
        if `r(N)' > 0 {
            estpost tabulate iso3_`suf' if num_nonmiss_`suf'<3 & report_`suf'==1, elabels
            esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("Country_`suf' with less than 3 observations from `beginYr' to `endYr'") ///
                varlabels(`e(labels)')
        }
    }
    quietly eststo : reg log_in_rev log_`x' i.id_iso3_o i.id_iso3_d ///
        i.id_iso3_o#c.year i.id_iso3_d#c.year if inrange(year,`beginYr',`endYr') ///
        & num_nonmiss_o>=3 & num_nonmiss_d>=3, ro
    countCountry
    predict temp_`x' if inrange(year,`beginYr',`endYr') ///
        & num_nonmiss_o>=3 & num_nonmiss_d>=3, xb
    estpost tabulate year if temp_`x' < . & in_rev_step4==. & num_nonmiss_o>=3 & num_nonmiss_d>=3
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("Additional missing in_rev that can be updated by extrapolation using `x' (home and host at least three obs)") ///
                varlabels(`e(labels)')    
    
    drop num_nonmiss_*
}
esttab * using "`csvPath'", append se r2 nogaps drop(*.id_*) ///
    scalars("num_home Num of home countries" "num_host Num of host countries") ///
    title("Estimate cross-sectional extrapolation for `direc' revenue 2001 - 2012")

gen in_rev_step5 = in_rev_step4
replace impute_in_rev = 50 ///
    if in_rev_step5 == . & temp_in_stock<.
replace in_rev_step5 = exp(temp_in_stock) if ///
    impute_in_rev == 50
replace impute_in_rev = 51 ///
    if in_rev_step5 == . & temp_out_rev<.
replace in_rev_step5 = exp(temp_out_rev) if ///
    impute_in_rev == 51
   
*******************************
**  Step 6 : time series extrapolation
*******************************
gen log_in_rev_step5 = log(in_rev_step5)
egen nonmiss_0112 = total(log_in_rev_step5<. & inrange(year,2001,2012)), by(id_pair)
egen nonmiss_9601 = total(log_in_rev_step5<. & inrange(year,1996,2001)), by(id_pair)
egen nonmiss_0611 = total(log_in_rev_step5<. & inrange(year,2006,2011)), by(id_pair)
egen tag_pair = tag(id_pair)

foreach yrRange in 0112 9601 0611 {
    estpost tabulate nonmiss_`yrRange' if report_o==1 & report_d==1 & tag_pair, elabels
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("number of nonmiss log values in year range `yrRange' before step 6") ///
                varlabels(`e(labels)')  
}                

** extrapolate between 2001 and 2012
local beginYr = 2001
local endYr = 2012
local minYr = 4
** !! slow !!
quietly reg log_in_rev_step5 i.id_pair i.id_iso3_o#c.year i.id_iso3_d#c.year i.year ///
    if nonmiss_0112 >= `minYr' & inrange(year,`beginYr',`endYr')
predict temp if nonmiss_0112 >= `minYr' & ///
    inrange(year,`beginYr',`endYr'), xb
gen in_rev_step6 = in_rev_step5
replace impute_in_rev = 60 if ///
    in_rev_step6==. & temp<.
replace in_rev_step6 = exp(temp) if impute_in_rev == 60

******************************
** impute additional zeros
******************************
** first identify missing values that cannot be zero (positive stock, or any positive inward/outward activities)
gen cannot_be_zero = (in_stock>0 & in_stock<.) | ///
      (in_n_ent_step4>0 & in_n_ent_step4<.) | ///
      (in_n_emp_step4>0 & in_n_emp_step4<.) | ///
      (out_rev_step4>0 & out_rev_step4<.) | ///
      (out_n_emp_step4>0 & out_n_emp_step4<.) | ///
      (out_n_ent_step4>0 & out_n_ent_step4<.) 
      
** identify runs of consecutive zeros and missing
preserve
keep iso3_o iso3_d report_o report_d id_pair
duplicates drop
tempfile id_pair
save `id_pair', replace
restore
fillin id_pair year // tsspell does not allow gaps
merge m:1 id_pair using `id_pair', nogen update
xtset id_pair year
tsspell, cond( in_rev_step6 == 0 | ///
    (in_rev_step6 == . & ~cannot_be_zero) )
gen zero_seq = _seq if in_rev_step6==0
egen first_zero_seq = min(zero_seq), by(id_pair _spell)
egen last_zero_seq = max(zero_seq), by(id_pair _spell)

gen in_rev_step7 = in_rev_step6
replace impute_in_rev = 70 if ///
    in_rev_step7 == . & _spell > 0 & _seq<last_zero_seq & _seq>first_zero_seq
replace in_rev_step7 = 0 if impute_in_rev == 70

drop nonmiss_* tag_pair
egen nonmiss_0112 = total(in_rev_step7<. & inrange(year,2001,2012)), by(id_pair)
egen nonmiss_9601 = total(in_rev_step7<. & inrange(year,1996,2001)), by(id_pair)
egen nonmiss_0611 = total(in_rev_step7<. & inrange(year,2006,2011)), by(id_pair)
egen tag_pair = tag(id_pair)

foreach yrRange in 0112 9601 0611 {
    estpost tabulate nonmiss_`yrRange' if report_o==1 & report_d==1 & tag_pair, elabels
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("number of nonmiss values in year range `yrRange' after step 6") ///
                varlabels(`e(labels)')  
}  

keep iso3_o iso3_d year in_stock in_rev_step* impute_in_rev out_stock out_rev
ds iso3_* year, not
egen anyInfo = rownonmiss(`r(varlist)'), strok
drop if anyInfo==0
drop anyInfo

capture label drop lab_impute_steps
do "$codeDir/lab_impute_steps.do"
label values impute_in_rev lab_impute_steps

compress
save "processed_data/extrap_bilat_in_rev.dta", replace


