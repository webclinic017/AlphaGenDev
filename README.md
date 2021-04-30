# AlphaGenDev


# Ad-hoc assumptions

## Data Construction

* SEC Data is available some days after it is accepted in EDGAR (2021-2009)- but since in Compustat we do not have the accepted data, we assume it is available 6 months after. 

## Signal Computation - No look ahead bias

* Keep only the most liquid stock for every CIK
* Remove from sample stocks with price<5 USD (think about it)
* Winsorize all variables including LHS Forwardret me bm prof cash avg_beta mret7 mret21 mret180
* Store xb u e and vol(e)
* Recompute it only on thursdays

## Backtest - No look ahead bias

### Universe of stocks to trade

* Non missing signal, xb, u
* Non missing idiosyncratic volatility
* Non missing Amihud Liquidity computed in the last 30 days
* Non missing market cap computed in the last 30 days

* Remove largest stocks, me>=p(90) - (Criteria from 2020)
* Remove 25% most illiquid stocks based on amihud illiquidity (maybe the top 5% mostly for short, 25% is too many, risk is more on the shorting side)
* Stock must have had a positive trading volume every trading day in the last month (this could also be quite strict, maybe work iwht percentages)

### Optimization

* Sort remaining companies on E[R]/vol(e) - Keep E[R] and impose conditions on vol, vol of y
* Assign a weight of 2% to the top 25
* Assign a weight of -2% to the bottom 25