/*


This file computes the trading signal avoiding look ahead bias
by using unly the required files. 

e.g. trading signal for April 24 - 2020 (trading ideally when the market opens) so can use only
closing prices up to April 23 -2020
- Load all data from 2020, drop anythinf from April 24 2020 forward
- append data from 2019, drop anything before April 24 2019

*/


// Loops over all dates
local start = mdy(1,1,2000)
local end   = mdy(4,15,2021)
forv t=`start'/`end'{
	if dow(`t')==4{
	qui{
	local PATH_TO_SEC_DATA: env PATH_TO_SEC_DATA
	//local t = mdy(4, 1, 2001)
	di %td `t'

	local y = year(`t') 
	// Upload data from the day
	use if t_day < `t' using "`PATH_TO_SEC_DATA'\information_set`y'.dta", clear
	local y=`y'-1

	append using "`PATH_TO_SEC_DATA'\information_set`y'.dta", force
	local tp=mdy(month(`t'), day(`t'), `y') // One year before 
	keep if t_day >= `tp'

	sum t_day
	noi: di "----------------------------------------------------------------------------------------"
	noi: di "Computing Signal for " %td `t'
	noi: di "Using Data"
	noi: di "From"
	noi: di %td `r(min)'
	noi: di "To"
	noi: di %td `r(max)'

	//drop if ret==. | me==. | bm==. | prof==. | cash==. | avg_beta==. | mret==.

	// Some cases for 2 different ciks we have different tickers, this could be because for foreign companies
	// we can have an American Depositary Receipt or an OTC, let's keep the one with the largest volume
	//order cik ticker t_day
	sort cik t_day volume
	by cik t_day: gen largest_volume=_n==_N
	keep if largest_volume
	drop largest_volume

	xtset cik t_day
	sort cik t_day // Creates the forward looking return
	by cik: gen Fexret=adjclose[_n+21]/adjclose[_n]-1

	drop if adjclose<=5
	// We need complete cases of firm characteristics
	foreach var in Fexret me bm prof cash avg_beta mret7 mret21 mret180{
	di "`var'"
		winsor2 `var', replace cuts(0.5 99.5)
	}
	// Winsorize right before the regression
	
	// 
	
	if `y'>=2008{
		local variables me bm prof cash avg_beta mret7 mret21 mret180 iyw_ret ixc_ret iyh_ret idu_ret iau_ret efg_ret
	}
	else{
		local variables me bm prof cash avg_beta mret7 mret21 mret180
	}
	xtreg Fexret `variables', fe
	
	foreach var in `variables'{
		gen _b_`var'=_b[`var']	
	}
	gen r2=e(r2)
	predict Eret if t_day==`t'-1 , xb
	predict fe, u

	by cik: carryforward fe, replace
	by cik: gen Fret=Eret + fe if t_day==`t'-1

	keep if t_day ==`t'-1
	keep if Eret  != . //technically also fe!=. Fret!=.
	replace t_day =`t'
	sort cik

	keep cik t_day ticker Fret Eret fe _b_* r2

	local yy=year(`t')
	local mm=month(`t')
	local dd=day(`t')
	save "`PATH_TO_SEC_DATA'\signals\signal`yy'-`mm'-`dd'.dta", replace

		}
	}
}
		