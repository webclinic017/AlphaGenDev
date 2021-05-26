/*

SP500 Constituents

SP500_aggregate.do

// Turns out the largest 500 companies might not be the ones in the index, so we use a more systematic approach

*/

local start = mdy(1,1,2010)
local end   = mdy(5,20,2021)
forv t=`start'/`end'{
	if dow(`t')==4{
	qui{
	//local t = mdy(4, 15, 2021)
	local PATH_TO_SEC_DATA: env PATH_TO_SEC_DATA
	noi: di "Computing ETFs for " %td `t'
	
	local yy=year(`t')
	local mm=month(`t')
	local dd=day(`t')
	
	use "`PATH_TO_SEC_DATA'\signals\v1\signal`yy'-`mm'-`dd'.dta", clear
	gen gics=""
	replace sic=floor(sic/10)
	do sic_to_gics
	
	// Create weighted return for each one
	replace gics=subinstr(gics, " ", "", .) 
	sort gics 
	drop if gics==""
	by gics: egen mcap=sum(me)
	gen walpha=(me/mcap)*Fret
	by gics: egen Fret_=sum(walpha)
	keep t_day Fret_ gics
	by gics: keep if _n==1
	reshape wide Fret_, i(t_day) j(gics) string
	save "`PATH_TO_SEC_DATA'\signals\v1\etfs`yy'-`mm'-`dd'.dta", replace
	
	}
	}
}
