
using Dates, Plots
using DataFrames
using CSV
using CategoricalArrays
cd("C:\\Users\\u109898\\Documents\\AlphaGenDev")
include("$(pwd())\\signal\\Backtest.jl")
min_date=Dates.Date(2014,1,1)
max_date=Dates.Date(2021,7,31)
time_span=min_date:Day(1):max_date # Iterator of dates
time_span=[t for t in time_span if dayofweek(t)==4]

N = 50
nls = [25, 35]
min_prices = [1.0, 2.0]
SEs = [0.15, 0.3, 0.5]
pliquids = [90, 95, 99]
DD_tiles = [5, 10, 15]
t_signal = ["xb", "xb+fe"]

parameters = []
revenues = []
for nl in nls
    for minprice in min_prices
        for se in SEs
            for pliquid in pliquids
                for DD_tile in DD_tiles
                    for type_signal in t_signal
                        try
                        println("$nl $minprice $se $pliquid $DD_tile $type_signal")
                        @time time, ret_portfolio, cum_ret, cum_benchmark, TICKERS, WEIGHTS,
                            nlongs, nshorts, ret_benchmark, problematic, ret_portfolio_rel= generate_backtest(min_date, 
                            max_date, time_span, frequency="w", verbose=false,  type_signal=type_signal, nl=nl, ns=N-nl, pliquid=pliquid, minprice=minprice, DD_tile=DD_tile, se=se);
                    
                        push!(parameters, "$nl $minprice $se $pliquid $DD_tile $type_signal")
                        df = DataFrame(date = time, ret_portfolio = ret_portfolio, ret_spy =ret_benchmark)
                        years = minimum(year.(time)):maximum(year.(time))
                    
                        
                        function quarter(d)
                            q = [ 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4 ]
                            return q[month(d)]
                        end

                        q_rp = []
                        date = []

                        for y in years
                            for q=1:4
                                try
                                    rets = ret_portfolio_rel[(year.(time) .== y) .* (quarter.(time) .== q)]
                                    push!(q_rp, maximum([prod(1.0 .+ rets)-1.0, 0.0]))
                                    push!(date, string(y,"Q",q))
                                catch

                                end
                            end
                        end




                        xtile(x; n=10) = levelcode.(cut(x, n, allowempty=true));


                        xt = xtile(q_rp)


                        hyp_money = xt*(30/8) .- 30/8


                        total_strategy = sum(hyp_money)
                        push!(revenues, total_strategy)
                    catch

                    end
                  
                    end
                end
            end
        end
    end
end
# plot() # Long Short
# plot!(time, cum_ret, label="Long Short 25-25 SP - p2-S5-l95-xb")
# plot!(time, cum_benchmark, label="SP500")
# plot!(legend =:topleft)




#CSV.write("LS25-25SP-p2-S5-l95-xb.csv", df)

#- Makes the backtest look more like with square point 
#- 1) Quarterly performance

