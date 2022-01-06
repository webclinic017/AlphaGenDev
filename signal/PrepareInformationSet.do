/*

This files converts the csv files that contain the information set year after year
into a Stata readable format so that data can be loaded and unloaded faster

*/

local PATH_TO_SEC_DATA: env PATH_TO_SEC_DATA
local max_y=real(substr("$S_DATE", -4, .))
forv y=2021/`max_y'{
di "`y'"
	qui{
		/* Helps debugging */
		//local PATH_TO_SEC_DATA: env PATH_TO_SEC_DATA
		//local y = 2021
		
		import delimited using "`PATH_TO_SEC_DATA'\information_set`y'.csv", clear
		
		// If gvkey doesnt exist, because we dont have compustat data as well, we create it
		
		capture confirm variable gvkey, exact
			if !_rc {
				display "gvkey exists in the sample"
		   }
		   else {
			  display "gvkey not present, creating empty"
			  gen gvkey = ""
		   }

		destring seqq,    force replace
		destring bm,      force replace
		destring beta,    force replace
		destring me,      force replace
		destring prof,    force replace
		destring cash,    force replace
		destring ret,     force replace
		destring mret7,   force replace
		destring mret21,  force replace
		destring mret180, force replace

		gen tt_day=date(t_day,  "YMD")
		format %td tt_day
		drop t_day
		rename tt_day t_day
		
		// Compute an industry p_e
		sort sic t_day
		// We include a variable for P/E
		gen p_e = prc7/(oiadpq/cshoq) // Price per earnings per share
		by sic t_day: egen avg_pe = mean(p_e)
		gen dev_pe = p_e-avg_pe // Deviation from industry
		
		
		
		gen MDR = li/(li+close*cshoq)
		by sic t_day: egen TL = mean(MDR)
		gen RL = MDR-TL
		
		gen mprof = oiadpq/(li+close*cshoq)
		by sic t_day: egen avg_prof = mean(mprof)
		gen dev_prof = mprof-avg_prof
		//**********************
		// LABELS
		//**********************
		
		label var         open "Open price of the day"     
		label var         high "Highest price of the day"     
		label var          low "Lowest price of the day"     
		label var        close "Closing price of the day"     
		label var     adjclose "Closing price after adjustments for splits and dividend distributions"     
		label var       volume "Number of shares traded"     
		label var       ticker "Ticker Symbol"           
		label var          ret "Daily Return computed relative to the adjusted close price in the last available trading day"     
		label var          cik "Central Index Key - SEC Identifier"     
		label var        ddate "Date that corresponds to the SEC disclosure used"     
		label var          atq "Total assets reported quarterly"     
		label var         cheq "Cash and Cash Equivalents reported quarterly"     
		label var        cshoq "Number of common shares outstanding - Source: SEC Filings"     
		label var       oiadpq "Operating Income After Depreciation"     
		label var         seqq "Book Value of Equity"     
		label var       source "Variable only relevant when SEC and Compustat Data coexist in the same file"           
		label var        gvkey "Compustat identifier, only relevant when SEC and Compustat data coexist"     
		label var      iyw_ret "Daily return on iShares U.S. Technology ETF (IYW) "     
		label var      ixc_ret "Daily return on iShares GLobal Energy ETF (IXC)"     
		label var      iyh_ret "Daily return on iShares U.S. Healthcare ETF (IYH)"     
		label var      idu_ret "Daily return on iShares U.S. Utilities ETF (IDU)"     
		label var      iju_ret "Return not used - discontinued"           
		label var      iau_ret "Daily return on iShares Gold Trues (IAU)"           
		label var     qual_ret "Daily return on iShares MSCI USA Quality Factor (QUAL)"           
		label var      efg_ret "Daily return on iShares MSCI EAFE Growth ETF (EFG)"           
		label var     gspc_ret "Daily return on ^GSPC - The price index tracking the SP500"     
		label var         beta "Rolling beta between ret and gspc_ret in the last 252 observations by cik"     
		label var           me "adjclose * cshoq"     
		label var     avg_size "30 day rolling average of (me) by cik"     
		label var           bm "seqq/avg_size"     
		label var      avg_ret "30 day rolling average of (ret) by cik"     
		label var         prof "oiadpq/atq"     
		label var         cash "cheq/atq"     
		label var     avg_beta "30 day rolling average of (beta)"     
		label var         prc7 "Price lagged 7 observations by cik"     
		label var        prc21 "Price lagged 21 observations by cik"     
		label var       prc180 "Price lagged 180 observations by cik"     
		label var        mret7 "adjclose/prc7 - 1"     
		label var       mret21 "adjclose/prc21 - 1"     
		label var      mret180 "adjclose/prc180 - 1"     
		label var         year "Year"     
		label var        t_day "Date with a daily format"  
		label var       dev_pe "Deviation of P/E from Industry"
		label var     dev_prof "Deviation of Profitability from Industry"
		label var           RL "Relative Leverage"

		save "`PATH_TO_SEC_DATA'\information_set`y'.dta", replace
	}
}

 