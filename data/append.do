cd "C:\Users\jfimb\Documents\AlphaGenDev\data"


local vars BTC-USD ///
			^VIX ///
			ZW=F ///
			CL=F ///
			RTY=F ///
			ES=F ///
			EURUSD=X ///
			GBPUSD=X ///
			GC=F ///
			IEUR ///
			IPAC ///
			HYG ///
			LQD ///
			ACWI /// 
			ACWV ///
			EWH ///
			TOK ///
			URTH ///
			TIP ///
			JPY=X ///
			YM=F ///
			NQ=F ///
			^N225 ///
			^TNX ///
			VFSTX
			
foreach var in `vars'{
	di "`var'"

	local url "https://query1.finance.yahoo.com/v7/finance/download/`var'?period1=631238400&period2=1620172800&interval=1d&events=history&includeAdjustedClose=true"
	import delimited using "`url'", clear varnames(1) 
	gen t_day=date(date, "YMD")
	format %td t_day
	drop date
	gen ticker="`var'"
	destring open high low close adjclose volume, replace force
	save "`var'", replace
}

use "TOK", clear	
foreach var in `vars'{

	append using "`var'"
	
}
duplicates drop ticker t_day, force

save to_append, replace
