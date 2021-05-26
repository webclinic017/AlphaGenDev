#--------------------------------------------------------
# PROGRAM NAME - .jl 
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.1    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGenDev
#
# USAGE - Analysis strategies relative to an index
#
# REQUIRES - Backtest output
#
# SYSTEM - All
#
# DATE - Apr 30 2021  (Fri) 11:06
#
# BUGS - Not known
#	
#
# DESCRIPTION - Compares the performance of a strategy relative
#               to a benchmark. 
#			
#			
#--------------------p=E[mx]------------------------------

using Dates
using Distributions
using Random
using Plots
using LaTeXStrings
using ProgressMeter

quarterAnalysis(time5, ret_portfolio5, ret_benchmark5)
holdingHorizons(time5, ret_portfolio5, ret_benchmark5)

function quarterAnalysis(time, ret_portfolio, ret_benchmark)
    # Time must be sorted
    # Quarter by Quarter we compare the returns
    # Unique months
    function t_quarter(d)
        q = [ 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4 ]
        "$(year(d))Q$(q[month(d)])"
    end
    quarters=unique(t_quarter.(time))

    rel_performance=[]
    for q in quarters
        t= t_quarter.(time) .== q
        j=size(ret_portfolio[t])[1]
        temp_cr= prod(1+ret_portfolio[t][i] for i=1:j) 
        temp_bm= prod(1+ret_benchmark[t][i] for i=1:j) 
        push!(rel_performance, (temp_cr-temp_bm)*100)

    end

    plot()
    bar!(quarters, rel_performance,  xrotation=45, label="", legend=:topleft)
    plot!(display=true)
    # Indicators - 
    # Fraction of quarters in which we beat the index
    fr = round(100*sum(rel_performance .> 0.0)/size(rel_performance)[1], digits=2)
    @info("The strategy beats the benchmark $fr% of quarters")

    # Average quarterly return, annualized and raw in excess of the index
    raw_q=mean(rel_performance)
    raw_a=((1+raw_q/100)^4 -1)*100
    @info("Average Quarter Return in Excess of the Benchmark $(round(raw_q, digits=2))")
    @info("Average Annual Return in Excess of the Benchmark $(round(raw_a, digits=2))")

    # Longest streak of quarters that we lose 
    ba= rel_performance .< 0
    ii=1
    jj=1 # Indices to where to look at 
    longest_subsequence=1
    for i=1:size(ba)[1]
        for j=i:size(ba)[1]
            subsequence_sum=sum(ba[i:j])
            feasible= subsequence_sum == j-i+1
            if feasible & (subsequence_sum > longest_subsequence)
                longest_subsequence=subsequence_sum
                ii=i
                jj=j
            end
        end
    end

    @info("Longest bad streak of  $longest_subsequence quarters between $(quarters[ii]) and $(quarters[jj])")
   
    plot!(show=true)

end



function holdingHorizons(time, ret_portfolio, ret_benchmark)
    

    # For testing purposes
    # returns=0.01 .+0.01 .*rand(Normal(),365*3)
    # benchmark=0.005 .+0.01 .*rand(Normal(),365*3)
    beatsMarket=[]
    beats=[]

    marketVolatility=[]
    returnVolatility=[]
    @showprogress for h in 0:52*2 #(size(returns)[1]-1)
       
        # We compute the cumulative return
        cum_ret      = [prod(1+ret_portfolio[i]   for i=j:(j+h)) for j=1:(size(ret_portfolio)[1]-h)]
        cum_benchmark= [prod(1+ret_benchmark[i] for i=j:(j+h)) for j=1:(size(ret_benchmark)[1]-h)]

        dailyEquivalent=[cum_ret[i]^(1/(1+h)) for i=1:(size(ret_portfolio)[1]-h)] .- 1.0
        dailyEquivalentB=[cum_benchmark[i]^(1/(1+h)) for i=1:(size(ret_benchmark)[1]-h)] .- 1.0
        push!(returnVolatility, round(100*std(dailyEquivalent),digits=2))
        push!(marketVolatility, round(100*std(dailyEquivalentB), digits=2))

        # What % beats the Market or has positive returns
        push!(beatsMarket, round(100*sum(cum_ret .> cum_benchmark)/size(cum_ret)[1],digits=2))
        push!(beats, round(100*sum(cum_ret .> 1.0)/size(cum_ret)[1],digits=2))
        
    end

    horizon=collect(0:(size(beats)[1]-1))
    plot()
    plot!(horizon, beatsMarket, label="Beats the market")
    plot!(horizon, beats, label="Positive return")
    xlabel!("Holding horizon")
    ylabel!(" % of time")
    vline!([1, 4, 52], label="")
    plot!(xticks=([1, 4, 52], ["1w", "1m", "1y"]))
    plot!(xrotation=90 )
    # savefig("$(f)horizons$ns")

    # horizon2=horizon[1:252]
    # plot()
    # plot!(horizon2, returnVolatility[1:252], label="Avg. Vol Strategy")
    # plot!(horizon2, marketVolatility[1:252], label="Avg. Vol Benchmark")
    # xlabel!("Holding horizon")
    # ylabel!("Daily Volatility")
    # plot!(xrotation=90 )
    # plot!(xticks=([7, 30, 30*6], ["1w", "1m", "6m"]))
    # savefig("$(f)volatility$ns")
end