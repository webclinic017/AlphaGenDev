include("signal/Backtest.jl")
using Dates, Plots
min_date=Dates.Date(2014,1,1)
max_date=Dates.Date(2021,10,1)
time_span=min_date:Day(1):max_date # Iterator of dates
time_span=[t for t in time_span if dayofweek(t)==4]

@time time, ret_portfolio, cum_ret, cum_benchmark, TICKERS, WEIGHTS, nlongs, nshorts, ret_benchmark, problematic= generate_backtest(min_date, 
max_date, time_span, frequency="w", verbose=true,  type_signal="xb", nl=25, ns=25, pliquid=95, minprice=1.0, DD_tile=1);

plot() # Long Short
plot!(time, cum_ret, label="Long Short 25-25 SP - p1-S1-l95-xb")
plot!(time, cum_benchmark, label="SP500")
plot!(legend =:topleft)

using DataFrames

df = DataFrame(date = time, ret_portfolio = ret_portfolio, ret_spy =ret_benchmark)

using CSV
CSV.write("LS25-25SP-p1-S1-l95-xb.csv", df)