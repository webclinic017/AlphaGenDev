include("signal/Backtest.jl")
using Dates, Plots
min_date=Dates.Date(2010,1,1)
max_date=Dates.Date(2021,4,15)
# @time time, ret_portfolio, cum_ret, cum_benchmark, TICKERS, WEIGHTS, nlongs, nshorts= generate_backtest(min_date, max_date);

@time time3, ret_portfolio3, cum_ret3, cum_benchmark3, TICKERS3, WEIGHTS3, nlongs3, nshorts3= generate_backtest(min_date, max_date);

# Winsorize the upper tail of the distribution of returns
# so that extreme events "pure luck" do not mess our results
histogram(ret_portfolio)

benchmark=CSV.read("$(PATH_TO_SEC_DATA)\\yahoo_finance\\data_etfs\\^GSPC.csv", DataFrame)

df=DataFrame(t_day=time2, ret=ret_portfolio2)
df=innerjoin(df, benchmark, on=:t_day)
ret_portfolio2=df.ret
ret_benchmark=df[!, "^GSPC_ret"]

cum_ret=[prod(1+ret_portfolio2[i] for i=1:j) for j=1:size(df)[1] ]
cum_benchmark=[prod(1+ret_benchmark[i] for i=1:j) for j=1:size(df)[1] ]
plot()
plot!(df.t_day, cum_ret, label="Long Square Point")
plot!(df.t_day, cum_benchmark, label="SP500")
plot!(legend = :topleft)

graphMonthlyReturns(df.t_day, ret_portfolio2-ret_benchmark)
plot!(show=true)