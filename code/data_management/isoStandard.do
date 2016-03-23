**************************************
** this do-file imports standardize ISO codes and 
** country names
**************************************

import excel countryName iso2 iso3 using "source_data/misc/isoCode.xlsx", clear cellrange(a2:c250)
save "processed_data/isoStandard.dta", replace
