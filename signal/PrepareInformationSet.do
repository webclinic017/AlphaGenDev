/*

This files converts the csv files that contain the information set year after year
into a Stata readable format so that data can be loaded and unloaded faster

*/

local PATH_TO_SEC_DATA: env PATH_TO_SEC_DATA
local max_y=real(substr("$S_DATE", -4, .))
forv y=1990/`max_y'{
di "`y'"
	qui{
		import delimited using "`PATH_TO_SEC_DATA'\information_set`y'.csv", clear

		destring seqq, force replace
		destring bm,   force replace
		destring beta, force replace
		destring me,   force replace
		destring prof, force replace
		destring cash, force replace
		destring ret, force replace
		destring mret7, force replace
		destring mret21, force replace
		destring mret180, force replace

		gen tt_day=date(t_day,  "YMD")
		format %td tt_day
		drop t_day
		rename tt_day t_day

		save "`PATH_TO_SEC_DATA'\information_set`y'.dta", replace
	}
}

 