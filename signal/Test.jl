include("signal/Backtest.jl")
using Dates, Plots
min_date=Dates.Date(2010,1,1)
max_date=Dates.Date(2010,1,15)
time_span=min_date:Day(1):max_date # Iterator of dates
#time_span=[t for t in time_span if dayofweek(t)==4]

@time time, ret_portfolio, cum_ret, cum_benchmark, TICKERS, WEIGHTS, nlongs, nshorts, ret_benchmark= generate_backtest(min_date, max_date, 
time_span, frequency="d");

# Winsorize the upper tail of the distribution of returns
# so that extreme events "pure luck" do not mess our results
histogram(ret_portfolio)

plot(time, cum_ret)
plot!(time, cum_benchmark)
plot!(legend=:topleft)

benchmark=CSV.read("$(PATH_TO_SEC_DATA)\\yahoo_finance\\data_etfs\\^GSPC.csv", DataFrame)

df=DataFrame(t_day=time, ret=ret_portfolio)
df=innerjoin(df, benchmark, on=:t_day)
ret_portfolio=df.ret
ret_benchmark=df[!, "^GSPC_ret"]
time=df.t_day
cum_ret=[prod(1+ret_portfolio3[i] for i=1:j) for j=1:size(df)[1] ]
cum_benchmark=[prod(1+ret_benchmark[i] for i=1:j) for j=1:size(df)[1] ]
plot()
plot!(df.t_day, cum_ret, label="Long Short Square Point")
plot!(df.t_day, cum_benchmark, label="SP500")
plot!(legend = :topleft)

graphMonthlyReturns(df.t_day, ret_portfolio-ret_benchmark)
plot!(show=true)