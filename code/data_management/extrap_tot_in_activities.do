**********************************************
** this do file extrapolates total inward MNE activities
** revenue and employment
** following the procedure described in the documentation
**********************************************

local csvPath = "$tableDir/extrap_tot_in_activities.csv"
capture rm "`csvPath'"

local xlsxPath = "$tableDir/potential_outliers_tot_in.xlsx"
capture rm "`xlsxPath'"

local outlier_thresh = 5

program drop _all
program countHost
    egen tag_host = tag(iso3_d) if e(sample)
    quietly sum tag_host
    estadd scalar num_host = r(sum)
    drop tag_host
end

************************************
** share calculated using FATS data
************************************
use "processed_data/nonfin_OECD_eurostat_activity.dta", clear
keep iso3_o iso3_d year es_in_rev_totXfin oecd_in_rev_totXfin
keep if inlist(iso3_o,"WRX","WRT")
reshape wide *in_rev*, i(iso3_d year) j(iso3_o) string
foreach x in es oecd {
    gen `x'_in_share = `x'_in_rev_totXfinWRX / `x'_in_rev_totXfinWRT
}

keep if year>=2000 & year<=2012
gen in_mp_share_fats = es_in_share if iso3_d~="PRT" // for PRT, use OECD series instead since it is the same as ES for common years but longer
replace in_mp_share_fats = oecd_in_share if iso3_d=="PRT"
egen nonmiss = total(in_mp_share_fats<.), by(iso3)

encode iso3, gen(id_iso3)
quietly regress in_mp_share_fats i.id_iso3 i.id_iso3#c.year
predict extrap_in_mp_share_fats if nonmiss>=4, xb

label var in_mp_share_fats "inward MP revenue shares calculated using FATS total output (WRT)"
keep iso3_d year in_mp_share_fats extrap_in_mp_share_fats
tempfile in_mp_share_fats
save `in_mp_share_fats', replace


************************************
** aggregate total inward stock for fugure use
************************************
use "processed_data/OECD_ES_UNCTAD_flow_stock.dta", clear
foreach x in UNCTAD oecd es {
    gen num_`x' = `x'_in_stock < .
}
collapse (sum) UNCTAD_in_stock oecd_in_stock es_in_stock num_*, by(iso3_d year)
foreach x in UNCTAD oecd es {
    replace `x'_in_stock = . if num_`x' == 0
}
drop num_*
tempfile in_stock
save `in_stock', replace

************ inward activities *************
use "processed_data/nonfin_OECD_eurostat_activity.dta", clear
drop *flag* *inv_tangi* *prod_v* *psn_cost* *purchase* *vadd* *unpaid* *out* // drop irrelevant variables
keep if iso3_o == "WRX"
ren iso3_d iso3
merge m:1 iso3 using "processed_data/activity_reporting_OECD_eurostat.dta", nogen keepusing(report_*_in)
keep if (report_oecd_in==1 | report_es_in==1)
drop iso3_o
ren iso3 iso3_d
fillin iso3_d year

******************************************
** Step 1: combine two variables on employment
******************************************
foreach s in oecd es {
    gen `s'_in_emp_totXfin = .
    gen source = ""
    capture confirm variable `s'_in_n_psn_emp_totXfin
    if ~_rc {
        disp as text "Use Number of Persons employed as primary source for `s' in."
        replace `s'_in_emp_totXfin = `s'_in_n_psn_emp_totXfin
        replace source = "psn_emp" if `s'_in_n_psn_emp_totXfin < .
        capture confirm variable `s'_in_n_emp_totXfin
        if ~_rc {
            disp as text "Supplement with Number of employmees."
            replace source = "emp" if `s'_in_emp_totXfin==. ///
                & `s'_in_n_emp_totXfin<.
            replace `s'_in_emp_totXfin = `s'_in_n_emp_totXfin ///
                if `s'_in_emp_totXfin==. ///
                & `s'_in_n_emp_totXfin<.
        }            
    }
    else {
        disp as text "Use Number of employees as primary source for `s' in."
        replace source = "emp" if `s'_in_n_emp_totXfin<.
        replace `s'_in_emp_totXfin = `s'_in_n_emp_totXfin
    }    
    
    estpost tabulate source, missing
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle varlabels(`e(labels)') ///
        title("Source of employment of `s' in")
    drop source
}

***********************************
** Step 2: drop time series anomalies ***
***********************************
ren (*_rev_totXfin *_emp_totXfin *n_ent_totXfin) (*_rev *_emp *ent) // shorten variable names
sort iso3_d year
encode iso3_d, gen(id_iso3_d)
xtset id_iso3_d year
foreach x in emp rev ent { 
foreach s in oecd es {
    egen temp = mean(log(`s'_in_`x')), by(iso3_d)
    gen dev_avg = log(`s'_in_`x') - temp
    gen tag = (dev_avg>`outlier_thresh' & dev_avg<.) | dev_avg<-`outlier_thresh'
    egen tot_tag = total(tag), by(iso3_d)
    gen gr_log = dev_avg - l.dev_avg
    gen f_gr_log = f.dev_avg - dev_avg
    
    gen outlier_condi_1 = (dev_avg > `outlier_thresh' & dev_avg<.) | dev_avg < - `outlier_thresh'
    gen outlier_condi_2 = (gr_log > `outlier_thresh'| gr_log<.) | (gr_log < - `outlier_thresh') | ///
                          (f_gr_log>`outlier_thresh'|f_gr_log<.) | (f_gr_log<-`outlier_thresh')
    
    gen tag_outlier_`s'_in_`x' = outlier_condi_1 & outlier_condi_2
    
    count if tot_tag > 0
    if `r(N)' > 0 {
        export excel iso3_d year *`s'_in_`x' dev_avg gr_log if tot_tag > 0 using "`xlsxPath'", ///
            sheetreplace sheet("`s'_tot_in_`x'") firstrow(variables)
    }        
    
    drop tag tot_tag dev_avg temp gr_log f_gr_log outlier_condi_*
    
}
}
** replace outlier as missing
foreach x in emp rev ent { 
foreach s in oecd es {
    replace `s'_in_`x' = . if tag_outlier_`s'_in_`x'
}
}
*** summary stats
estpost summarize tag_outlier_*, quietly 
esttab . using "`csvPath'", append cells("sum count") noobs ///
    title("Number of outliers")
    
drop id_iso3_d tag_*
    
*********************************
** step 3: supplement missing data in 
** Eurostat with OECD
** step 4 : impute zeros using FDI stocks
** or employment and number of enterprises
*********************************

merge 1:1 iso3_d year using `in_stock', keep(master match) nogen
gen in_stock = es_in_stock
gen source_in_stock = "es" if es_in_stock<.
replace source_in_stock = "oecd" if oecd_in_stock<. & in_stock==.
replace in_stock = oecd_in_stock if source_in_stock == "oecd"
estpost tabulate source_in_stock, missing
esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle ///
    title("Sources for in stock") varlabels(`e(labels)')    


foreach x in rev emp ent {
    
    gen in_`x' = es_in_`x' if report_es_in==1
    replace in_`x' = oecd_in_`x' if report_oecd_in==1 & ///
        report_es_in~=1
    gen impute_in_`x' = "orginal_es_or_oecd" if in_`x'<.
    
    ** step 3
    replace impute_in_`x' = "es_suppBy_oecd_to_0" if report_es_in==1 & ///
        in_`x'==. & oecd_in_`x' == 0
    replace in_`x' = 0 if impute_in_`x' == "es_oecd_to_0"
    
    replace impute_in_`x' = "es_suppBy_oecd_to_+" if report_es_in==1 & ///
        in_`x'==. & oecd_in_`x' > 0 & oecd_in_`x'<.
    replace in_`x' = oecd_in_`x' if impute_in_`x' == "es_suppBy_oecd_to_+"

    ** step 4
    replace impute_in_`x' = "non-positive FDI stock" ///
        if in_stock<=0 & in_`x'==.
    replace in_`x' = 0 if impute_in_`x' == "non-positive FDI stock"        
    
}

replace impute_in_rev = "zero employment and number of enterprises" ///
    if in_rev == . & ///
       ( (in_emp == 0 & in_ent==.) | ///
       (in_emp == . & in_ent==0) | ///
       (in_emp ==0 & in_ent==0) )
       
replace in_rev = 0 if impute_in_rev == "zero employment and number of enterprises"

keep iso3_d year in_emp in_rev in_ent in_stock impute_*

foreach x in rev emp ent {
    estpost tabulate impute_in_`x', elabels
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle ///
        title("Impute cases after Step 4 : in `x'") varlabels(`e(labels)')
}

*******************************************
** step 5 : cross-section regression extrapolation
*******************************************

gen log_in_rev = log(in_rev)
gen log_in_emp = log(in_emp)
gen log_in_stock = log(in_stock)

encode iso3_d, gen(id_iso3_d)

** try regression by different year range
eststo clear
foreach beginYr in 1996 2002 2008 {
foreach x in in_emp in_stock {
    local endYr = min(`beginYr' + 5 , 2012)
        egen num_nonmiss_d = total(year<=`endYr' & year>=`beginYr' ///
            & log_in_rev<. & log_`x'<.), by(iso3_d)
        count if num_nonmiss_d <3
        if `r(N)' > 0 {
            estpost tabulate iso3_d if num_nonmiss_d<3, elabels
            esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("Country_d with less than 3 observations from `beginYr' to `endYr' - in_rev and `x'") ///
                varlabels(`e(labels)')
        }

    quietly eststo : reg log_in_rev log_`x' i.id_iso3_d ///
        i.id_iso3_d#c.year i.year if inrange(year,`beginYr',`endYr') ///
        & num_nonmiss_d>=3, ro
    countHost
    estadd local yr_range "`beginYr'-`endYr'"
    drop num_nonmiss_*
}
}

esttab * using "`csvPath'", append se r2 nogaps drop(*.id_*) ///
    scalars("num_host Num of host countries" "yr_range year range" ) ///
    title("Estimate cross-sectional extrapolation for in revenue every 6 years")
    
** pick one year range    
local beginYr = 1995
local endYr = 2012
eststo clear
foreach x in in_stock in_emp {
    egen num_nonmiss_d = total(year<=`endYr' & year>=`beginYr' ///
        & log_in_rev<. & log_`x'<.), by(iso3_d)
    count if num_nonmiss_d<3
    if `r(N)' > 0 {
        estpost tabulate iso3_d if num_nonmiss_d<3, elabels
        esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
            title("Country_d with less than 3 observations from `beginYr' to `endYr'") ///
            varlabels(`e(labels)')
    }
    quietly eststo : reg log_in_rev log_`x' i.id_iso3_d ///
        i.id_iso3_d#c.year i.year if inrange(year,`beginYr',`endYr') ///
        & num_nonmiss_d>=3, ro
    countHost
    predict temp_`x' if inrange(year,`beginYr',`endYr') ///
        & num_nonmiss_d>=3, xb
    estpost tabulate year if temp_`x' < . & in_rev==. & num_nonmiss_d>=3
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("Additional missing in_rev that can be updated by extrapolation using `x' (host at least three obs)") ///
                varlabels(`e(labels)')    
    drop num_nonmiss_*
}

esttab * using "`csvPath'", append se r2 nogaps drop(*.id_*) ///
    scalars("num_home Num of home countries" "num_host Num of host countries") ///
    title("Estimate cross-sectional extrapolation for in revenue 2001 - 2012")

gen in_rev_step5 = in_rev
replace impute_in_rev = "cross-section extrap using inward stock" ///
    if in_rev_step5 == . & temp_in_stock<.
replace in_rev_step5 = exp(temp_in_stock) if ///
    impute_in_rev == "cross-section extrap using inward stock"
replace impute_in_rev = "cross-section extrap using inward employment" ///
    if in_rev_step5 == . & temp_in_emp<.
replace in_rev_step5 = exp(temp_in_emp) if ///
    impute_in_rev == "cross-section extrap using inward employment"
   
*******************************
**  Step 6 : time series extrapolation
*******************************
gen log_in_rev_step5 = log(in_rev_step5)
egen nonmiss_0112 = total(log_in_rev_step5<. & inrange(year,2001,2012)), by(iso3_d)
egen nonmiss_9601 = total(log_in_rev_step5<. & inrange(year,1996,2001)), by(iso3_d)
egen nonmiss_0611 = total(log_in_rev_step5<. & inrange(year,2006,2011)), by(iso3_d)
egen tag_iso3_d = tag(iso3_d)
foreach yrRange in 0112 9601 0611 {
    estpost tabulate nonmiss_`yrRange' if tag_iso3_d, elabels
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("number of nonmiss log values in year range `yrRange' before step 6") ///
                varlabels(`e(labels)')  
}                

** extrapolate between 2001 and 2012
local beginYr = 2001
local endYr = 2012
local minYr = 4
quietly reg log_in_rev_step5 i.id_iso3_d i.id_iso3_d#c.year i.year ///
    if nonmiss_0112 >= `minYr' & inrange(year,`beginYr',`endYr')
predict temp if nonmiss_0112 >= `minYr' & ///
    inrange(year,`beginYr',`endYr'), xb
gen in_rev_step6 = in_rev_step5
replace impute_in_rev = "time-series extrap 2001-2012" if ///
    in_rev_step6==. & temp<.
replace in_rev_step6 = exp(temp) if impute_in_rev == "time-series extrap 2001-2012"

******************************
** impute additional zeros
******************************
** first identify missing values that cannot be zero (positive stock, or any positive inward/outward activities)
gen cannot_be_zero = (in_stock>0 & in_stock<.) | ///
      (in_ent>0 & in_ent<.) | ///
      (in_emp>0 & in_emp<.)
      
** identify runs of consecutive zeros and missing
xtset id_iso3_d year
tsspell, cond( in_rev_step6 == 0 | ///
    (in_rev_step6 == . & ~cannot_be_zero) )
gen zero_seq = _seq if in_rev_step6==0
egen first_zero_seq = min(zero_seq), by(id_iso3_d _spell)
egen last_zero_seq = max(zero_seq), by(id_iso3_d _spell)

replace impute_in_rev = "runs of missing between zeros" if ///
    in_rev_step6 == . & _spell > 0 & _seq<last_zero_seq & _seq>first_zero_seq
replace in_rev_step6 = 0 if impute_in_rev == "runs of missing between zeros"

codebook in_rev_step6 if year>=2006 & year<=2011
drop nonmiss_* tag_iso3_d
egen nonmiss_0112 = total(in_rev_step6<. & inrange(year,2001,2012)), by(iso3_d)
egen nonmiss_9601 = total(in_rev_step6<. & inrange(year,1996,2001)), by(iso3_d)
egen nonmiss_0611 = total(in_rev_step6<. & inrange(year,2006,2011)), by(iso3_d)
egen tag_iso3_d = tag(iso3_d)

foreach yrRange in 0112 9601 0611 {
    estpost tabulate nonmiss_`yrRange' if tag_iso3_d, elabels
    esttab . using "`csvPath'", append cell(b) unstack noobs nonumber nomtitle noabbrev ///
                title("number of nonmiss values in year range `yrRange' after step 6") ///
                varlabels(`e(labels)')  
}      

keep iso3_d year in_stock in_rev in_emp in_ent in_rev_step* impute_in_rev 
ds iso3_d year, not
egen anyInfo = rownonmiss(`r(varlist)'), strok
drop if anyInfo==0
drop anyInfo

merge 1:1 iso3_d year using `in_mp_share_fats', nogen

compress
save "processed_data/extrap_tot_in_activities.dta", replace


