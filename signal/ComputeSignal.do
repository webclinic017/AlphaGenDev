/*


This file computes the trading signal avoiding look ahead bias
by using unly the required files. 

e.g. trading signal for April 24 - 2020 (trading ideally when the market opens) so can use only
closing prices up to April 23 -2020
- Load all data from 2020, drop anythinf from April 24 2020 forward
- append data from 2019, drop anything before April 24 2019


Change, When computing the signal of a thursday i cannot just do t-1 because it might happen that that friday was  aholiday, e.g. July 4 2001
the predict if t_day=t-1 needs to be replaced with the last available info, e.g. sum t_day if t_day<t use r(max)

Log: 
	The filter of 5 USD might cause some problems in some occassions, so I moved the filter to 1 USD 08/05/2021
	
	Instead of filtering we put the conditional in the regression, and then predict everything, that way those with less than 5 could have a signal but the parameters wont be affected

*/




// Loops over all dates
local start = mdy(1,1,2010)
local end   = mdy(1,13,2022)
forv t=`end'/`end'{
	if dow(`t')==4{
	//qui{
	local PATH_TO_SEC_DATA: env PATH_TO_SEC_DATA
	//local t = mdy(1, 6, 2022)
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

	//drop if adjclose < 1
	// We need complete cases of firm characteristics
	foreach var in Fexret me bm prof cash avg_beta mret7 mret21 mret180 RL dev_pe dev_prof{
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
	//local variables me bm prof cash avg_beta mret7 mret21 mret180
	
	local variables_extended `variables' RL dev_pe dev_prof
	xtreg Fexret `variables' if adjclose >=5, fe
	
	
	
	foreach var in `variables'{
		gen _b_`var'=_b[`var']	
	}
	gen r2=e(r2)
	
	sum t_day if t_day < `t'
	local last_day =`r(max)'
	di %td `last_day'
	predict Eret if t_day== `last_day', xb
	predict fe, u
	
	predict resid, resid
	egen sd_resid = sd(resid), by(cik)
	//Idiosyncratic volatility

	by cik: carryforward fe, replace
	by cik: gen Fret=Eret + fe if t_day==`last_day'
	
	// Extensions of the model, e.g. use RL and other deviations, let's call it complete
	
	xtreg Fexret `variables_extended' if adjclose >=5, fe
	
	predict Eret_extended if t_day== `last_day', xb
	predict fe_extended, u
	
	predict resid_extended, resid
	egen sd_resid_extended = sd(resid), by(cik)
	//Idiosyncratic volatility

	by cik: carryforward fe_extended, replace
	by cik: gen Fret_extended=Eret_extended + fe_extended if t_day==`last_day'
	
	di %td `last_day'

	keep if t_day ==`last_day'
	keep if Fret  != . 
	replace t_day =`t'
	sort cik

	//keep cik t_day ticker Fret Eret fe _b_* r2 sd_resid

	local yy=year(`t')
	local mm=month(`t')
	local dd=day(`t')
	save "`PATH_TO_SEC_DATA'\signals\v1\signal`yy'-`mm'-`dd'.dta", replace
	
	sort me
	keep if _n >_N-500
	egen mcap=sum(me)
	gen walpha=(me/mcap)*Fret
	egen Fret_sp500=sum(walpha)
	keep t_day Fret_sp500
	keep if _n==1
	save "`PATH_TO_SEC_DATA'\signals\v1\sp500`yy'-`mm'-`dd'.dta", replace

		//}
	}
}
		