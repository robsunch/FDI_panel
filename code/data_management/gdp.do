***************************************************************
** compile current price GDP in US dollars
** add Taiwan since World Bank does not include it
** see checkData/check_gdp.do and 
**	   Output/tables/checkData/check_gdp.txt
** for comparison between the WB series and 
** data from Karabarbounis and Neiman
**
** input:  source_data/misc/Taiwan_GDP.xlsx
**	       .../misc/ny.gdp.mktp.cn_Indicator_en_excel_v2.xls
**
** output: processed_data/countryVar/gdp.dta
***************************************************************

log close _all
log using "$logDir/gdp.smcl", replace

*** Taiwan GDP data
import excel year gdp_twnDollar gdp using "source_data/misc/Taiwan_GDP.xlsx", sheet("matrix") cellrange(a4:c66) clear
destring year, replace
keep year gdp
replace gdp = gdp * 1e6 // million dollars to dollars
gen iso3 = "TWN"
tempfile twn_gdp
save `twn_gdp'

*** world bank GDP data, current US$
import excel using "source_data/misc/ny.gdp.mktp.cd_Indicator_en_excel_v2.xls", sheet("Data") cellrange(A1:BF251) clear
foreach x of varlist E-BF {
	local yr = `x'[3]
	ren `x' gdp`yr'
	}
ren B iso3	
keep iso3 gdp*
drop in 1/3
reshape long gdp, i(iso3) j(year) string
destring year gdp, replace

keep iso3 year gdp
append using `twn_gdp'
drop if missing(gdp)
label var gdp "GDP in current USD from World Bank except for Taiwan"

save "processed_data/gdp.dta", replace

log close _all