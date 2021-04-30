#--------------------------------------------------------
# PROGRAM NAME - Simulation.jl
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.1.1    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGen Backtests
#
# USAGE - This module performs a simulation of a backtest Given
#         an estimation of the signal e.g. E[R] and the optimization
#         parameters
#
# REQUIRES - Information about expected returns/variances
#
# SYSTEM - All
#
# DATE - Mar 27 2021  (Sat) 17:36
#
# BUGS - Not known
#	
#
# DESCRIPTION - Performs a simulation of the performance of an investmentObjective
#               strategy based on a estimation of expected returns and possibly variances
#
# Log - 
#      0.1 18/04/2021: This version takes into account the possibility of look ahead bias, by loading separetely
#      the required file with the new signal
#			
#			
#--------------------p=E[mx]------------------------------


using LinearAlgebra
using Distributions 
using Roots
using NLsolve 
using DataFrames 
using Dates
using Query 
using CSV
using NamedArrays
using Plots
using Missings
using JLD
using StatsBase
using BusinessDays
using ProgressMeter
using Gurobi
using FixedEffectModels
using LaTeXStrings
using StatFiles

PATH_TO_SEC_DATA=ENV["PATH_TO_SEC_DATA"]

# For debugging
# include("signal/PortfolioManagement.jl")
# include("signal/Tools.jl")

include("PortfolioManagement.jl")
include("Tools.jl")

# Hold for the new version


function generate_backtest(min_date, max_date)

    
    time_span=min_date:Day(1):max_date # Iterator of dates
    n=size(time_span)[1]
    p = Progress(n)

    # Some first check, we need to have a signal available at least 7 days from the beginning of the backtest, otherwise we cannot really compare backtests
    signal_files=readdir("$(PATH_TO_SEC_DATA)\\signals\\v1")
    signal_files=[Dates.Date(file[7:end-4]) for file in signal_files]

    @assert minimum(time_span)>=minimum(signal_files)

    # preload information sets to access them faster but still in a separate location
    miny=minimum(year.(signal_files))
    maxy=maximum(year.(signal_files))
    information_sets=[CSV.read("$(PATH_TO_SEC_DATA)\\information_set$y.csv", DataFrame) for y in miny:1:maxy] 
                
    #Information on the benchmark
    benchmark=CSV.read("$(PATH_TO_SEC_DATA)\\yahoo_finance\\data_etfs\\^GSPC.csv", DataFrame)
    ret_portfolio=[]
    tickers=[]
    weights=[]
    y_var=[]

    TICKERS=[]
    WEIGHTS=[]

    env=Gurobi.Env()
    #@showprogress  
   

    cum_ret, time, cum_benchmark, nlongs, nshorts=[], [], [], [], []

    info=Dict()
    info["winner"]          = "" 
    info["winner_ret"]      = 0.0
    info["winner_date"]     = min_date
    info["winner_position"] = 0.0
    info["loser"]           = "" 
    info["loser_ret"]       = 0.0
    info["loser_date"]      = min_date
    info["loser_position"] = 0.0
    info["best_return"]     = 0.0
    info["best_date"]       = min_date
    info["worst_return"]    = 0.0
    info["worst_date"]      = min_date

    # # Data from SIC codes
    # sics=CSV.read(SICFILE[], DataFrame)
    for t in time_span
        if size(cum_ret)[1]>0
        ProgressMeter.next!(p; showvalues = [(:Date,t), 
                                             (:Cumulative_Return,      cum_ret[end]),
                                             (:Cumulative_Benchmark,   cum_benchmark[end]),
                                             (:Winner,           info["winner"]),
                                             (:Winner_return,    info["winner_ret"]),
                                             (:Winner_date,      info["winner_date"]),
                                             (:Winner_position,  info["winner_position"]),
                                             (:Loser,            info["loser"]),
                                             (:Loser_return,     info["loser_ret"]),
                                             (:Loser_date,       info["loser_date"]),
                                             (:Loser_position,   info["loser_position"]),
                                             (:Best_return,      info["best_return"]),
                                             (:Best_date,        info["best_date"]),
                                             (:Worst_return,     info["worst_return"]),
                                             (:Worst_date,       info["worst_date"]),
                                             (:HWM,              maximum(cum_ret)),
                                             (:LWM,              minimum(cum_ret))
                                             ])
        end
        # Data from returns and prices of the day before
        y=year(t)
        
        information_set=information_sets[y-miny+1]
        #Remove anything after t strictly, we are just thinking what stocks to buy
        df=@from i in information_set begin
                @where  i.t_day < t #! NOTE, THE STRICT < SIGN
                @select {i.t_day, i.open, i.adjclose, i.volume, i.ticker, i.ret, i.cshoq, i.sic, i.beta}
                @collect DataFrame                
        end

        try
            df_a=@from i in information_sets[y-miny] begin
                @select {i.t_day, i.open, i.adjclose, i.volume, i.ticker, i.ret, i.cshoq, i.sic, i.beta}
                @collect DataFrame     
            end

            df= append!(df, df_a) 
        catch
        end

        maxt=maximum(df.t_day)
        #println(t)
        @assert t>maxt # Makse sure we add the one before and not after
        #We keep the last month of observations to compue a measure of liquidity
        
        df=@from i in df begin
            @where (maxt-Day(15)<=i.t_day<=maxt) & !isnan(i.volume) & !isnan(i.adjclose) &!isnan(i.ret)
            @select {i.ticker,i.t_day, i.open, i.adjclose, i.volume, i.ret, i.cshoq, i.sic, i.beta}
            @collect DataFrame        
        end

        sort!(df, [order(:ticker), order(:t_day)])
        # drop nans
        #df.illiq = abs.(df.ret .* df.adjclose .* df.cshoq)./(df.adjclose .* df.volume)
        df.illiq = abs.(df.ret)./(df.adjclose .* df.volume)
        df.me=df.adjclose .* df.cshoq
        gdf=groupby(df, [:ticker])
        df_combined=combine(gdf, :illiq => mean, :volume => minimum, :me => last, :beta => last, :sic => last)

        # Merge with the signal data before removing highly illiquid stocks
        # this is because some of them have been filtered already by only estimating the model
        # e.g. with stocks with prc >=5

        # What is my signal? Either signalt.dta or the closest one
        # Finds the closest file from above of time t
       
        date_signal=signal_files[findall(x -> x == maximum(signal_files[signal_files .<= t]), signal_files)][1]
        yyyy=year(date_signal)
        mm=month(date_signal)
        dd=day(date_signal)
        signal=DataFrame(load("$(PATH_TO_SEC_DATA)\\signals\\v1\\signal$yyyy-$mm-$dd.dta"))
       
        signal=signal[!, [:ticker, :Fret, :Eret, :fe, :sd_resid]]

        df=innerjoin(signal, df_combined, on = :ticker)
        sort!(df, [order(:ticker)])
        #.* (!).(isnan.(df.illiq_mean))
        df=df[completecases(df) , :]
        #df=df[df.sd_resid .> 0.0, :]
        # Make sure we also have info on σ\_ϵ   
        #println(t)
        df=df[(!).(isnan.(df.illiq_mean)) .* (!).(isnan.(df.me_last)) , :]
        #df=df[df.me_last .<= percentile(df.me_last, 90), :]
        df=df[df.illiq_mean .<= percentile(df.illiq_mean, 95), :] #!!!!!!!!!! 
        #df=df[df.sd_resid .<= percentile(df.sd_resid, 90), :]
        #df=df[df.illiq_mean .<= percentile(df.illiq_mean, 50), :]
        #!df=df[df.volume_minimum .> 0.0, :] # We also drop stocks with very low dollar volume

        # Now we need one digit sic codes
        
        # Returns on time t #! I should check expost if trades are done when volume is 0
      

        if dayofweek(t)==4
            
            α=df.Eret #!
            σ=df.sd_resid
            df.one_sic=convert.(Int64, floor.(df.sic_last ./ 1000))
            β=df.beta_last
            S=cate_to_mat(df.one_sic)
            #S=[]
            #ω_value, y_value, yp_value, yn_value= optimizationSquarePoint(α , σ, env, only_long=true)
            #ω_value, y_value, yp_value, yn_value= simpleLongShort(α , σ)
            #**************************************************************************************
            ub=1/10
            lb=-1/10
            
            use_sector_exposure=false
            βₘ=0.0
            
            N=size(α)[1]
            u_up=ones(N,1)*ub
            u_down= ones(N,1)*lb
            use_signal=1
            GRB_ENV=env
            free_longshort=false
            βn=[]
            #println(t)

            L=2.0
            nLong=25
            nShort=25
            # end
            ω_value, ωc_value, ω_long_value, ω_short_value = rebalance_optimization(α ,    #! 
                                                                                    β, 
                                                                                    βn,
                                                                                    u_up, 
                                                                                    u_down, 
                                                                                    L,
                                                                                    S,
                                                                                    GRB_ENV=GRB_ENV,
                                                                                    use_sector_exposure=use_sector_exposure, 
                                                                                    βₘ=βₘ, 
                                                                                    βnasdaq=-99,
                                                                                    nlong=nLong,
                                                                                    nshort=nShort,
                                                                                    free_longshort=free_longshort)
            
            ω_value .= ω_value .* 100 
            y_value = ω_value .!= 0.0
            #**************************************************************************************
          
            y_value=convert.(Bool, y_value)
            tickers=df.ticker[y_value]
            weights=ω_value[y_value]
            y_var=y_value
            
        end
        
       

        df_portfolio=DataFrame(ticker=tickers, ω=weights)

        df_ret=information_sets[year(t)-miny+1]
        df_ret=@from i in df_ret begin
            @where i.t_day == t
            @select {i.ticker, i.ret}
            @collect DataFrame   
        end

        #Merge the returns
        df=leftjoin(df_portfolio, df_ret, on=:ticker)
        if size(df)[1]>0

            push!(nlongs, sum(df.ω .> 0.0 ))
            push!(nshorts, sum(df.ω .< 0.0 ))
            df.ret[ismissing.(df.ret)] .= 0.0

            # Updates winners and lossers to display

            # Winner is the stock with the largest gain so far
            gains=[df.ω[i] > 0.0 ? df.ret[i] : -df.ret[i] for i=1:size(df)[1]]
            
            i_w=argmax(gains)
            i_l=argmin(gains)

            if round(gains[i_w]*100, digits=2) > info["winner_ret"]
            info["winner"]=df.ticker[i_w]
            info["winner_ret"]=round(gains[i_w]*100, digits=2)
            info["winner_date"]=t
            info["winner_position"]=round(df.ω[i_w], digits=2)
            end

            if round(gains[i_l]*100, digits=2) < info["loser_ret"]
            info["loser"]=df.ticker[i_l]
            info["loser_ret"]=round(gains[i_l]*100, digits=2)
            info["loser_date"]=t
            info["loser_position"]=round(df.ω[i_l], digits=2)
            end

            port_ret=sum(df.ret[i]*df.ω[i]/100 for i=1:size(df)[1])

            push!(ret_portfolio, port_ret)

            if port_ret*100 > info["best_return"]
                info["best_return"] = round(port_ret*100, digits=2)
                info["best_date"]   = t
            end

            if port_ret*100 < info["worst_return"]
                info["worst_return"] = round(port_ret*100, digits=2)
                info["worst_date"]   = t
            end

            sdf=benchmark[benchmark.t_day .==t, :]
            sp500_ret=0.0
            if size(sdf)[1]>0
            sp500_ret=sdf[!, "^GSPC_ret"][1]
            end

            if size(cum_ret)[1]>0
                push!(cum_ret, cum_ret[end]*(1+port_ret))
                push!(cum_benchmark, cum_benchmark[end]*(1+sp500_ret))
            else
                push!(cum_ret, (1+port_ret))
                push!(cum_benchmark, 1+sp500_ret)
            end

            push!(time, t)
        end

        push!(TICKERS, tickers)
        push!(WEIGHTS, weights)

    end

return time, ret_portfolio, cum_ret, cum_benchmark, TICKERS, WEIGHTS, nlongs, nshorts

# plot()
# plot!(time, cum_ret)
# plot!(time, cum_benchmark)

end



# benchmark=CSV.read("$(PATH_TO_SEC_DATA)\\yahoo_finance\\data_etfs\\^GSPC.csv", DataFrame)
# df=DataFrame(t_day=time, ret=ret_portfolio)
# df=innerjoin(df, benchmark, on=:t_day)
# ret_portfolio=df.ret
# ret_benchmark=df[!, "^GSPC_ret"]
# cum_ret=[prod(1+df.ret[i] for i=1:j) for j=1:size(df)[1] ]
# cum_benchmark=[prod(1+ret_benchmark[i] for i=1:j) for j=1:size(df)[1] ]
# plot()
# plot!(df.t_day, cum_ret, label="Long Short Ret")
# plot!(df.t_day, cum_benchmark, label="SP500")

# time_span=df.t_day

# graphMonthlyReturns(time_span, ret_portfolio-ret_benchmark)

# Monthly




"""
    `placeOrder(q, ub, lb, L, use_sector_exposure)`

Given data `q` and optimization parameters `ub lb L` rebalances the portfolio
"""
function placeOrder(q, ub, lb, L, use_sector_exposure, βₘ, βnasdaq, nLong, nShort, use_signal, GRB_ENV, free_longshort)
    one_sic=q.one_sic
    S=cate_to_mat(one_sic)    
    α=q.Fret
    β=q.beta
    βn=[] #q._b_ret_nasdaq
    N=length(α)
    u_up=ones(N,1)*ub
    u_down= ones(N,1)*lb
    t=q.t_day[1]
    #println(t)
   
    #We could have less than 50 stocks at some point
    if N<(nShort+nLong) && nShort==0
        nLong=N
    end
    ω_value, ωc_value, ω_long_value, ω_short_value = rebalance_optimization(α, 
                                                                            β, 
                                                                            βn,
                                                                            u_up, 
                                                                            u_down, 
                                                                            L,
                                                                            S,
                                                                            GRB_ENV=GRB_ENV,
                                                                            use_sector_exposure=use_sector_exposure, 
                                                                            βₘ=βₘ, 
                                                                            βnasdaq=βnasdaq,
                                                                            nlong=nLong,
                                                                            nshort=nShort,
                                                                            free_longshort=free_longshort)

  
    return ω_value

end



# """
#     `backtest(min_date, max_date, frequency; stop_losses=false, use_sector_exposure=false, dow="Friday",
#     t_dates=[], L=2.0, lb=-1/10, ub=1/10)`

# Performs a backtest of the end of day performance between `min_date` and `max_date` at `frequency`

# """
# function backtest(min_date, max_date, frequency; stop_losses=false, use_sector_exposure=false, dow="Thursday",
#                                                  t_dates=[], L=2.0, lb=-1/10, ub=1/10, sector=:No, βₘ=0.0, βnasdaq=-99,  
#                                                  nLong=25, nShort=25, use_signal=1, free_longshort=false)                                                 
                                                 
                                                 
# DFILE=Ref{String}("C:/Users/jfimb/Dropbox/2021-03-18.csv")
# BFILE=Ref{String}("C:/Users/jfimb/Dropbox/sp500.csv")
# SICFILE=Ref{String}("C:/Users/jfimb/Dropbox/sics.csv")
# BIYW=Ref{String}("data/IYW.csv")
# BIYH=Ref{String}("data/IYH.csv")
# SECTORS=Ref{String}("data/10_Industry_Portfolios_Daily.csv")
# GRB_ENV = Gurobi.Env()

# out_file = open("file_$(frequency).out", "w")
# # Business days in the U.S. 
# BusinessDays.initcache(:USSettlement)

# #***********************************************************************************
# #** Parameters for the back testing
# #rebalance= "end-month"
# #rebalance="daily"
# rebalance=frequency
# stop_loss=0.05 #If the price falls (increases) when going long (short) we cancel a position
             
# signal=CSV.read(DFILE[], DataFrame)
# #signal=CSV.read("2020-12-01_pe.csv")
# signal.t_day=string_to_date.(signal.t_day)

# # Calculations are faster if we filter only the dates we need
# signal=@from i in signal begin
#     @where  min_date <= i.t_day <= max_date
#     @select {i.symbol, i.Fret1, i.Fret2, i.Fret3, i.comnam, i.beta, i.volume, i.ret, i.prc, i.t_day, i.cik, i.one_sic}
#     @collect DataFrame                
# end
# #For the backtesting we winsorize data based on its liquidity and returns so backtests are conservative
# signal.amihud=abs.(signal.ret .*signal.prc)./(signal.prc .*signal.volume)

# # Remove NaNs 
# signal=filter(:amihud => x -> !any(f -> f(x), (ismissing, isnothing, isnan)), signal)
# #histogram(nonan(signal.amihud))
# per=percentile(signal.amihud, 50)
# # We get rid of anything above that
# signal=signal[signal.amihud .< per, :]
# # Let's drop the very illiquid stocks
# #histogram(nonan(signal.amihud))
# # WInsorize returns

# #** Compute β for nasdaq
# # nasdaq=CSV.read("data/^IXIC.csv", DataFrame)
# # # Drop null obs
# # for name in names(nasdaq)[2:end]
# # nasdaq[!, name] = map(x->begin val = tryparse(Float64, x)
# #                            ifelse(typeof(val) == Nothing, missing, val)
# #                       end, nasdaq[!, name])
# # end

# # # Create returns of the index
# # nasdaq[!, :retNasdaq] .= 0.0
# # temp=(nasdaq[2:end, :Close]-nasdaq[1:end-1, :Close])./nasdaq[1:end-1, :Close]

# # for i=1:size(temp)[1]
# #     if ismissing(temp[i])
# #         nasdaq[(i+1), :retNasdaq] = 0.0
# #     else
# #         nasdaq[(i+1), :retNasdaq] = temp[i]
# #     end
# # end

# # # rename date column
# # rename!(nasdaq,:Date => :t_day)
# # nasdaq=nasdaq[!, [:t_day, :retNasdaq]]
# # Check the SIC for each one
# sics=CSV.read(SICFILE[], DataFrame)
# signal=innerjoin(signal, sics, on = :cik)
# #signal=innerjoin(signal, nasdaq, on = :t_day)

# signal.one_sic=convert.(Int64, floor.(signal.sic/1000))
# signal.two_sic=convert.(Int64, floor.(signal.sic/100))

# if sector!=:No # This means no focus of sector 
#     @info("Filtering companies for sector $sector")
#     if sector!=:Other
#         signal=@from i in signal begin
#             @where  i.sic in sicCodes(sector)
#             @select {i.symbol, i.Fret1, i.Fret2, i.Fret3, i.comnam, i.beta, i.one_sic, i.two_sic, i.ret, i.prc, i.t_day, i.sic}
#             @collect DataFrame                
#         end
#         # What to use as benchmark?
#         sp500=CSV.read(SECTORS[], DataFrame)
#         sp500.sp500_ret=sp500[:, sector]/100
#         sp500.t_day=yyyymmddToDate.(sp500.Column1)
#         sp500=sp500[completecases(sp500), :]
#         sp500=sp500[:, [:t_day, :sp500_ret]]
#     else
#         # all other sics
#         allSectors=[:NoDur, :Durbl, :Manuf, :Enrgy, :HiTec, :Telcm, :Shops, :Hlth, :Utils]
#         allSics=[]
#         for sec in allSectors
#             append!(allSics, sicCodes(sec))
#         end

#         signal=@from i in signal begin
#             @where  i.sic ∉ allSics
#             @select {i.symbol, i.Fret1, i.Fret2, i.Fret3, i.comnam, i.beta, i.one_sic, i.two_sic, i.ret, i.prc, i.t_day, i.sic}
#             @collect DataFrame                
#         end

#         sp500=CSV.read(SECTORS[], DataFrame)
#         sp500.sp500_ret=sp500[:, sector]/100
#         sp500.t_day=yyyymmddToDate.(sp500.Column1)
#         sp500=sp500[completecases(sp500), :]
#         sp500=sp500[:, [:t_day, :sp500_ret]]
#     end
# else
#     sp500=CSV.read(BFILE[], DataFrame)
#     sp500.t_day=string_to_date.(sp500.t_day)
#     sp500=sp500[completecases(sp500), :]
# end
# #histogram(nonan(signal.ret))
# #min_date=minimum(signal.t_day)#
# #max_date=maximum(signal.t_day)

# # What if our filter is another one? e.g. low volatility stocks

# # Computes a rolling window volatility
# # First we select the appropriate elements on a rolling basis, this could be vectorized but for consistency I have it verbose

# # w=365 # Window for the volatility
# # #signal[!, :vol] =  missings(Float64, size(signal)[1]) # Initializes the column as a mix of float and missing

# # macro newcol(df, col)
# #     return :( $df[!, $col]=missings(Float64, size($df)[1]) )
# # end

# # @newcol signal :vol 

# #!** Initial Attempt takes too much time, discard
# # @showprogress for t in min_date+Day(w):Day(1):max_date
# #     # Unique ciks at time t
# #     uciks=unique(signal[signal.t_day .== t , :cik])
# #     # Get w days of info so we dont select data each time
# #     tempSignal=signal[(signal.t_day .<= t).* (signal.t_day .>= t-Day(w)), [:cik, :ret]]
# #     for c in uciks
# #         signal[(signal.cik .== c) .* (signal.t_day .== t), :vol] .= std(tempSignal[tempSignal.cik .== c, :ret])
# #     end
# # end

# # #!** Second attempt, parallelize
# # l = Threads.SpinLock()
# # p = Progress(size(collect(min_date+Day(w):Day(1):max_date))[1])
# # Threads.@threads for t in min_date+Day(w):Day(1):max_date
# #     # Unique ciks at time t
# #     uciks=unique(signal[signal.t_day .== t , :cik])
# #     # Get w days of info so we dont select data each time
# #     tempSignal=signal[(signal.t_day .<= t).* (signal.t_day .>= t-Day(w)), [:cik, :ret]]
# #     for c in uciks
# #         signal[(signal.cik .== c) .* (signal.t_day .== t), :vol] .= std(tempSignal[tempSignal.cik .== c, :ret])
# #     end   
# #     Threads.lock(l)
# #     ProgressMeter.next!(p)
# #     Threads.unlock(l)
# # end

# # Every day we filter, we need to do this after computing the volatilities
# # signal=filter(:vol=> x -> !any(f -> f(x), (ismissing, isnothing, isnan)), signal) # Remove obs with no volatility computed

# # # Mean before the filter
# # temp_signal=signal
# # bm=mean(signal.vol)
# # @showprogress for t in minimum(signal.t_day):Day(1):max_date
# #     # Unique ciks at time t
# #     try
# #     vols=signal[signal.t_day .== t , :vol]
# #     #histogram(nonan(signal.amihud))
# #     per=percentile(vols, 90)
# #     # We get rid of anything above that on that day
# #     cond=.!((signal.vol .< per) .* (signal.t_day .== t))
# #     signal=signal[ cond, :]
# #     catch e
# #     end
# # end
# # am=mean(signal.vol)

# # @info("Average Volatility reduced from $(round(bm*100, digits=2)) to $(round(am*100, digits=2))")


# time_span=max(min_date, minimum(signal.t_day)):Day(1):max_date # Iterator of dates


# cum_ret=[1.0] #cumulative return
# cum_benchmark=[1.0]
# ret_portfolio=[] # Return of the portfolio
# ret_portfolio_long=[] # Return of the portfolio
# ret_portfolio_short=[] # Return of the portfolio
# ret_benchmark=[]
# last_weights=[] # Store weights from the day before
# last_assets=[]
# prices_bought=[] # Prices at which we bought the stocks

# sec_exposure_pos=[]
# sec_exposure_neg=[]
# dates_rebalance=[]
#  # Here I am going to record how the concentration of each sector exists, either negative or positive
# fd=time_span[1]
# last_traded=Date(year(fd), month(fd),day(fd))-Month(1)
# n=size(time_span)[1]
# p = Progress(n)
# for t in time_span
    
#     lastReturn=cum_ret[end]
#     lastBenchmark=cum_benchmark[end]
    
#     q=@from i in signal begin
#         @where  !ismissing(i.prc) && !ismissing(i.ret) && !ismissing(i.Fret1) && !ismissing(i.Fret2) && !ismissing(i.Fret3) && !ismissing(i.beta) && i.t_day==t
#         @select {i.symbol, i.Fret1, i.Fret2, i.Fret3, i.comnam, i.beta, i.one_sic, i.ret, i.prc, i.t_day, i.two_sic}
#         @collect DataFrame                
#     end 
    
#     # How many companies?
#     ncompanies=size(q)[1]
#     ProgressMeter.next!(p; showvalues = [(:date,t), 
#     (:portfolio, lastReturn),
#     (:benchmark, lastBenchmark),
#     (:N, ncompanies)])
    
#     sp=@from i in sp500 begin
#         @where i.t_day==t
#         @select {i.sp500_ret}
#         @collect DataFrame                
#     end
#     #If there is no data that day we move to the next day, returns go to zero and cumrets remain the same
#     #println("$t-$(length(q.symbol))-$(length(sp.sp500_ret))-$(isbday(BusinessDays.USSettlement(), t))")
#     if size(q)[1]==0 || size(sp)[1]==0

#         push!(ret_portfolio, 0.0)
#         push!(ret_benchmark, 0.0)
#         push!(cum_ret, cum_ret[end])
#         push!(cum_benchmark, cum_benchmark[end])

#         #@info("No data for $t - continuing...")
#         continue
#     end

    
    
    
#     if size(q)[1]>0 && length(sp.sp500_ret)>0 && isbday(BusinessDays.USSettlement(), t)
#         # For some reason there are sometimes missing values left
#         q=q[completecases(q), :]
#         println(out_file, "-------------------------------------------------------------------------------")
#         println(out_file, "Date: $t")
#         # First we need to make sure it is a business day, otherwise portfolio remains the same
#         long=last_assets[last_weights.>0.0]
#         short=last_assets[last_weights.<0.0]
#         long=[l => last_weights[last_assets.==l]*100 for l in long ]
#         short=[s => last_weights[last_assets.==s]*100 for s in short ]
#         port_value=round(cum_ret[end], digits=3)
#         println(out_file, "Initial Portfolio Value: $port_value")
#         # Gets data at time t
#         println(out_file, "Long Positions")
#         println(out_file, long)
#         println(out_file, "Short Positions")
#         println(out_file, short)
#         q.ticker=Symbol.(q.symbol)
#         # Check the stop order, profit until it triggered
#         t_ret=0.0
#         t_ret_long=0.0
#         t_ret_short=0.0
#         if length(prices_bought)>0
#             for (i_s, stock) in enumerate(last_assets)
#                 if last_weights[i_s]!=0.0 # If I have a position on this asset
#                     try
#                         price_b=prices_bought[last_assets.==stock][1] # Price bought
#                         price_n=q.prc[q.ticker.==stock][1]
#                         t_ret=t_ret+last_weights[i_s]*q[q.ticker.==stock, :].ret[1]
#                         if stop_losses
#                             if last_weights[i_s]>0.0 && ((price_n/price_b)-1.0)<-stop_loss
#                                 last_weights[i_s]=0.0
#                             elseif last_weights[i_s]<0.0 && ((price_n/price_b)-1.0)>stop_loss
#                                 last_weights[i_s]=0.0
#                             end
#                         end
#                     catch 
#                         continue
#                     end
#                 end

#                 if last_weights[i_s]>0.0 # If I have a long position on this asset
#                     try
#                         price_b=prices_bought[last_assets.==stock][1] # Price bought
#                         price_n=q.prc[q.ticker.==stock][1]
#                         t_ret_long=t_ret_long+last_weights[i_s]*q[q.ticker.==stock, :].ret[1]
#                         if stop_losses
#                             if last_weights[i_s]>0.0 && ((price_n/price_b)-1.0)<-stop_loss
#                                 last_weights[i_s]=0.0
#                             elseif last_weights[i_s]<0.0 && ((price_n/price_b)-1.0)>stop_loss
#                                 last_weights[i_s]=0.0
#                             end
#                         end
#                     catch 
#                         continue
#                     end
#                 end

#                 if last_weights[i_s]<0.0 # If I have a short position on this asset
#                     try
#                         price_b=prices_bought[last_assets.==stock][1] # Price bought
#                         price_n=q.prc[q.ticker.==stock][1]
#                         t_ret_short=t_ret_short+last_weights[i_s]*q[q.ticker.==stock, :].ret[1]
#                         if stop_losses
#                             if last_weights[i_s]>0.0 && ((price_n/price_b)-1.0)<-stop_loss
#                                 last_weights[i_s]=0.0
#                             elseif last_weights[i_s]<0.0 && ((price_n/price_b)-1.0)>stop_loss
#                                 last_weights[i_s]=0.0
#                             end
#                         end
#                     catch 
#                         continue
#                     end
#                 end
#             end
#         end

            

#         # No need to update values cause the money liquidated goes to cash
            
#         push!(ret_portfolio, t_ret)
#         push!(ret_portfolio_long, t_ret_long)
#         push!(ret_portfolio_short, t_ret_short)
#         push!(ret_benchmark, sp.sp500_ret)
#         push!(cum_ret, cum_ret[end]*(1+t_ret))
#         push!(cum_benchmark, cum_benchmark[end]*(1.0+ sp.sp500_ret[1]))
#         port_value=round(cum_ret[end], digits=3)
#         day_ret=round(t_ret*100, digits=1)
#         println(out_file, "Final portfolio value: $port_value.")
#         println(out_file, "Return = $day_ret %. ")

#         # How do we know if it is the end of the month? If the number of business days between that
#         # day and the beginning of the next month is either 2, 1, or 0. This is because some holidays are not in Business Days
#         #try
#             # Computes the distance since the last rebalancing
#         last_rebalancing=Dates.value(t-last_traded)
#             # Last traded for monthly rebalancing has to have happenede at least 28 days ago
#         if size(t_dates)[1]==0 && rebalance== "monthly" && bdayscount(:USSettlement, t, lastdayofmonth(t)+Day(1))<=2 && last_rebalancing>=28
                
#             println(out_file, "End of the month $t, rebalancing...")
#             last_weights=placeOrder(q, ub, lb, L, use_sector_exposure,βₘ, βnasdaq, nLong, nShort, use_signal,GRB_ENV, free_longshort)
#             last_assets=q.ticker
#             prices_bought=q.prc
#             last_traded=t

#             sics_portfolio_pos = [q.two_sic[i] for i in 1:size(q.two_sic)[1] if last_weights[i]>0 ]
#             sics_portfolio_neg = [q.two_sic[i] for i in 1:size(q.two_sic)[1] if last_weights[i]<0 ]

#             push!(sec_exposure_pos, sics_portfolio_pos)
#             push!(sec_exposure_neg, sics_portfolio_neg)

#             push!(dates_rebalance, t)
#         elseif size(t_dates)[1]==0 && rebalance=="daily"
                
#             println(out_file, "Day $t, rebalancing...")
#             last_weights=placeOrder(q, ub, lb, L, use_sector_exposure,βₘ, βnasdaq, nLong, nShort, use_signal, GRB_ENV, free_longshort)
#             last_assets=q.ticker
#             prices_bought=q.prc
#             last_traded=t
#             sics_portfolio_pos = [q.two_sic[i] for i in 1:size(q.two_sic)[1] if last_weights[i]>0 ]
#             sics_portfolio_neg = [q.two_sic[i] for i in 1:size(q.two_sic)[1] if last_weights[i]<0 ]

#             push!(sec_exposure_pos, sics_portfolio_pos)
#             push!(sec_exposure_neg, sics_portfolio_neg)
#             push!(dates_rebalance, t)
#         elseif size(t_dates)[1]==0 && rebalance=="weekly" && Dates.dayname(t)==dow && isbday(BusinessDays.USSettlement(), t)
        
#             println(out_file, "Day $t weekly rebalancing...")
#             last_weights=placeOrder(q, ub, lb, L, use_sector_exposure,βₘ, βnasdaq, nLong, nShort, use_signal, GRB_ENV, free_longshort)
#             last_assets=q.ticker
#             prices_bought=q.prc
#             last_traded=t
#             sics_portfolio_pos = [q.two_sic[i] for i in 1:size(q.two_sic)[1] if last_weights[i]>0 ]
#             sics_portfolio_neg = [q.two_sic[i] for i in 1:size(q.two_sic)[1] if last_weights[i]<0 ]

#             push!(sec_exposure_pos, sics_portfolio_pos)
#             push!(sec_exposure_neg, sics_portfolio_neg)
#             push!(dates_rebalance, t)
#         elseif size(t_dates)[1]>0 && (t in t_dates)

#             println(out_file, "Date $t, rebalancing...")  
#             last_weights=placeOrder(q, ub, lb, L, use_sector_exposure,βₘ, βnasdaq, nLong, nShort, use_signal, GRB_ENV, free_longshort)
#             last_assets=q.ticker
#             prices_bought=q.prc
#             last_traded=t
#             sics_portfolio_pos = [q.two_sic[i] for i in 1:size(q.two_sic)[1] if last_weights[i]>0 ]
#             sics_portfolio_neg = [q.two_sic[i] for i in 1:size(q.two_sic)[1] if last_weights[i]<0 ]

#             push!(sec_exposure_pos, sics_portfolio_pos)
#             push!(sec_exposure_neg, sics_portfolio_neg)
#             push!(dates_rebalance, t)
#         end     
#     else
#         println(out_file, "-------------------------------------------------------------------------------")
#         println(out_file, "Date: $t - Market Closed")
#         push!(ret_portfolio, 0.0)
#         push!(cum_ret, cum_ret[end])
#         push!(ret_benchmark, 0.0)
#         push!(cum_benchmark, cum_benchmark[end])
#     end

# end

# println(out_file, "*********************************************************************")
# println(out_file, "Summary")
# mret=((1+mean(ret_portfolio))^30-1)*100
# println(out_file, "Monthly E[R] (Assuming 30 Days) = $mret %")
# sdret=100*std(ret_portfolio)*sqrt(30)
# println(out_file, "Monthly σ[R] (Daily * √30) = $sdret %")
# SR=sqrt(252)*(mean(ret_portfolio)-0.012/100)/std(ret_portfolio)
# println(out_file, "Annual S.R. (Daily * √360) = $SR")
# println(out_file, "*********************************************************************")

# close(out_file)

# return time_span, cum_ret, ret_portfolio, ret_portfolio_long, ret_portfolio_short, ret_benchmark, cum_benchmark, sec_exposure_pos, sec_exposure_neg,  dates_rebalance

# end

# #end #module


# function createBacktestWebsite(time_span, cum_ret, ret_portfolio, ret_benchmark, cum_benchmark, last_assets, last_weights)


#     #Could I get the sics of the last position=
#     # df=DataFrame(cik=last_assets, weight=last_weights)
#     # sics=CSV.read(SICFILE[], DataFrame)
#     # df=innerjoin(df, sics, on = :cik)
#     founder="Filippo Ippolito"

#     strategyName= "AlphaGen High Volatility Portfolio"
#     strategyCode="HVP"

#     wdate = Dates.format(maximum(time_span), "u d, yyyy")
#     mdate = Dates.format(minimum(time_span), "u d, yyyy")
#     totalReturn=round((cum_ret[end]-1)*100, digits=2)
#     # <span class="ytd-change-arrow-up"></span>
#     # <span class="header-nav-data">
#     #     $totalReturn%
#     # </span>
#     stringSign=totalReturn>=0.0 ? "up" : "down"
#     totalReturn= """
#     <span class="ytd-change-arrow-$stringSign"></span>
#     <span class="header-nav-data">
#         $totalReturn%
#     </span>
#     """
#     #!** daily return
#     # <span class="nav-change-arrow-up"></span>
#     # <!-- <span class="nav-change-arrow-down"></span> -->
#     # <span class="header-nav-data">
#     #     (1000%)
#     # </span>
#     notzero=false
#     k=0
#     lastRetDaily=0
#     while !notzero
#         if ret_portfolio[end-k]!=0.0
#             lastRetDaily=ret_portfolio[end-k]
#             notzero=true
#         else
#             k=k+1
#         end
#     end
#     lastRetDaily=round(lastRetDaily*100, digits=2)
#     stringSign=lastRetDaily>=0.0 ? "up" : "down"
#     dret="""    
#     <span class="nav-change-arrow-$stringSign"></span>
#     <span class="header-nav-data">
#         ($lastRetDaily%)
#     </span>
#     """

#     whyStrategy="""
#     <p>1. Smart exposure to volatile U.S. companies</p>
#     <p> </p>
#     <p>2. Low cost access to a long-short active strategy using high volatility stocks</p>
#     <p> </p>
#     <p>3. Use at the core of your portfolio to get exposure to volatility</p>
#     """

#     investmentObjective="""
#     The AlphaGen $strategyCode seeks to maximize expected returns in a Long-Short strategy by actively holding 
#     50 high volatility stocks while seeking to maintain market neutrality, and a maximum concentration of 10%. 
#     """

#     #Hypothetical growth of 10000
#     time_spanv2=[minimum(time_span)-Day(1)]
#     append!(time_spanv2, time_span)
#     df=DataFrame(Date=time_spanv2, Strategy=cum_ret*10000, Benchmark=cum_benchmark*10000)
#     urlcsv="C:/Users/jfimb/OneDrive/Documentos/GitHub/jfimbett.github.io/assets/strategy.csv"
#     CSV.write(urlcsv, df)

#     nameBenchmark="SP500"
#     annualized=zeros(4)
#     annualizedB=zeros(4)

#     horizons=[1,3,5,9]
#     k=1
#     for h in horizons
#         annualized[k]  = round((cum_ret[end]-cum_ret[end-365*h])*100/h, digits=2)
#         annualizedB[k]= round((cum_benchmark[end]-cum_benchmark[end-365*h])*100/h, digits=2)
#         k=k+1
#     end

#     oneYearAnnualized  = annualized[1]
#     oneYearAnnualizedB = annualizedB[1]

#     threeYearAnnualized  = annualized[2]
#     threeYearAnnualizedB = annualizedB[2]

#     fiveYearAnnualized  = annualized[3]
#     fiveYearAnnualizedB = annualizedB[3]

#     tenYearAnnualized  = annualized[4]
#     tenYearAnnualizedB = annualizedB[4]


#     #Cumulative performance
    
#     oneMonth=round((cum_ret[end]-cum_ret[end-30*1])*100, digits=2)
#     oneBMonth=round((cum_benchmark[end]-cum_benchmark[end-30*1])*100, digits=2)
#     threeMonth =round((cum_ret[end]-cum_ret[end-30*3])*100, digits=2)
#     threeBMonth=round((cum_benchmark[end]-cum_benchmark[end-30*3])*100, digits=2)
#     sixMonth=round((cum_ret[end]-cum_ret[end-30*6])*100, digits=2)
#     sixBMonth=round((cum_benchmark[end]-cum_benchmark[end-30*6])*100, digits=2)
#     oneYear =round((cum_ret[end]-cum_ret[end-30*12])*100, digits=2)
#     oneBYear=round((cum_benchmark[end]-cum_benchmark[end-30*12])*100, digits=2)
#     threeYear =round((cum_ret[end]-cum_ret[end-30*36])*100, digits=2)
#     threeBYear =round((cum_benchmark[end]-cum_benchmark[end-30*36])*100, digits=2)
#     fiveYear =round((cum_ret[end]-cum_ret[end-30*60])*100, digits=2)
#     fiveBYear =round((cum_benchmark[end]-cum_benchmark[end-30*60])*100, digits=2)
#     nineYear =round((cum_ret[end]-cum_ret[1])*100, digits=2)
#     nineBYear=round((cum_benchmark[end]-cum_benchmark[1])*100, digits=2)


#     #Returns year after year
#     years=[2016, 2017, 2018, 2019, 2020]
#     avgs=zeros(size(years)[1])
#     avBgs=zeros(size(years)[1])
#     ret_benchmark=[ret[1] for ret in ret_benchmark]
#     for (iy,y) in enumerate(years)
#         subrets=ret_portfolio[Dates.year.(time_span) .== y]
#         # Compute cum rets for that year
#         cumrets=[prod(1+subrets[i] for i in 1:j) for j in 1:size(subrets)[1]]
#         avgs[iy]=round((cumrets[end]-cumrets[1])*100, digits=2)

#         subrets=ret_benchmark[Dates.year.(time_span) .== y]
#         # Compute cum rets for that year
#         cumrets=[prod(1+subrets[i] for i in 1:j) for j in 1:size(subrets)[1]]
#         avBgs[iy]=round((cumrets[end]-cumrets[1])*100, digits=2)
#     end

#     ret2016=avgs[1]
#     ret2017=avgs[2]
#     ret2018=avgs[3]
#     ret2019=avgs[4]
#     ret2020=avgs[5]
#     retB2016=avBgs[1]
#     retB2017=avBgs[2]
#     retB2018=avBgs[3]
#     retB2019=avBgs[4]
#     retB2020=avBgs[5]

#     marketBeta=round(cov(ret_benchmark[(end-365*3):end], ret_portfolio[(end-365*3):end])/var(ret_benchmark[(end-365*3):end]), digits=2)
#     standardDeviation=round(std(ret_portfolio[(end-365*3):end])*sqrt(365)*100, digits=2)
#     f = open("C:/Users/jfimb/OneDrive/Documentos/GitHub/jfimbett.github.io/assets/alphagen.html")
#     s = read(f, String);
#     s=replace(s, "\$founder" => "$founder");
#     s=replace(s, "\$wdate" => "$wdate");
#     s=replace(s, "\$dret" => "$dret");
#     s=replace(s, "\$mdate" => "$mdate");
#     s=replace(s, "\$totalReturn" => "$totalReturn");
#     s=replace(s, "\$strategyName" => "$strategyName");
#     s=replace(s, "\$strategyCode" => "$strategyCode");
#     s=replace(s, "\$whyStrategy" => "$whyStrategy");
#     s=replace(s, "\$investmentObjective" => "$investmentObjective");
#     s=replace(s, "\$urlcsv" => "$urlcsv");
#     s=replace(s, "\$nameBenchmark" => "$nameBenchmark");
#     s=replace(s, "\$oneYearAnnualized" => "$oneYearAnnualized"  );
#     s=replace(s, "\$oneYearBnnualized" => "$oneYearAnnualizedB");
#     s=replace(s, "\$threeYearAnnualized" => "$threeYearAnnualized"  );
#     s=replace(s, "\$threeYearBnnualized" => "$threeYearAnnualizedB" );
#     s=replace(s, "\$fiveYearAnnualized" => "$fiveYearAnnualized"  );
#     s=replace(s, "\$fiveYearBnnualized" => "$fiveYearAnnualizedB" );
#     s=replace(s, "\$tenYearAnnualized" => "$tenYearAnnualized");
#     s=replace(s, "\$tenYearBnnualized" => "$tenYearAnnualizedB");

#     s=replace(s, "\$oneMonth" => "$oneMonth");
#     s=replace(s, "\$oneBMonth" => "$oneBMonth");
#     s=replace(s, "\$threeMonth" => "$threeMonth");
#     s=replace(s, "\$threeBMonth" => "$threeBMonth");
#     s=replace(s, "\$sixMonth" => "$sixMonth");
#     s=replace(s, "\$sixBMonth" => "$sixBMonth");
#     s=replace(s, "\$oneYear" => "$oneYear");
#     s=replace(s, "\$oneBYear" => "$oneBYear");
#     s=replace(s, "\$threeYear" => "$threeYear");
#     s=replace(s, "\$threeBYear" => "$threeBYear");
#     s=replace(s, "\$fiveYear" => "$fiveYear");
#     s=replace(s, "\$fiveBYear" => "$fiveBYear");
#     s=replace(s, "\$nineYear" => "$nineYear");
#     s=replace(s, "\$nineBYear" => "$nineBYear");

#     s=replace(s, "\$ret2016" => "$ret2016");
#     s=replace(s, "\$ret2017" => "$ret2017");
#     s=replace(s, "\$ret2018" => "$ret2018");
#     s=replace(s, "\$ret2019" => "$ret2019");
#     s=replace(s, "\$ret2020" => "$ret2020");
#     s=replace(s, "\$retB2016" => "$retB2016");
#     s=replace(s, "\$retB2017" => "$retB2017");
#     s=replace(s, "\$retB2018" => "$retB2018");
#     s=replace(s, "\$retB2019" => "$retB2019");
#     s=replace(s, "\$retB2020" => "$retB2020");
#     s=replace(s, "\$marketBeta" => "$marketBeta");
#     s=replace(s, "\$standardDeviation" => "$standardDeviation");
    
#     close(f)
    
#     f=open("C:/Users/jfimb/OneDrive/Documentos/GitHub/jfimbett.github.io/assets/alphagenTest.html", "w")
#     write(f, eval(s));
#     close(f)


# end


# function rollingLoadings(time_span, benchmark, returns, cum_benchmark, cum_ret, w, ns, f)
 
#     plot()
#     plot!(time_span,cum_ret[2:end], label="Cum Return")
#     plot!(time_span, cum_benchmark[2:end], label="Cum Benchmark $ns")
#     ylabel!("Daily cum return")
#     title!("Full backtest")
#     savefig("$(f)backtest$ns")

#     R=DataFrame(t_day=time_span, ret=returns, benchmark=benchmark)
#     #Let's get data from market and factor returns
#     df=CSV.read("data/F-F_Research_Data_5_Factors_2x3_daily.csv", DataFrame);
#     mom=CSV.read("data/F-F_Momentum_Factor_daily.csv", DataFrame)
#     # First we convert dates
#     df[!, :t_day]= yyyymmddToDate.(df[!, :Column1]);
#     mom[!, :t_day]=yyyymmddToDate.(mom[!, :Column1]);
#     R=innerjoin(R, df, on=:t_day)
#     R=innerjoin(R, mom, on=:t_day, makeunique=true)
#     rename!(R, Symbol("Mkt-RF") => :mktrf )
#     rename!(R, Symbol("Mom   ") => :Mom)
    
#     R[!, :exRet]=R[!, :ret]*100-R[!, :RF]
#     ff5fm=@formula(exRet ~  mktrf+SMB+HML+RMW+CMA+Mom)
#     capm=@formula(exRet ~  mktrf)
#     β_capm, β_capm_mktrf=[],[]
#     β,      β_mktrf     =[],[]
#     s,      s_smb       =[],[]
#     h,      h_hml       =[],[]
#     r,      r_rmw       =[],[]
#     c,      c_cma       =[],[]
#     m,      m_mom       =[],[]
#     α_capm=[]
#     α_ff5fm=[]
#     t_day=R[w:end, :t_day]
#     R[!, :α]=[-99.0 for i in R[!, :t_day]]
#     for t in w:size(R[!, :t_day])[1]
#         temp=R[(t-w+1):t, :]
#         results=reg(temp,ff5fm, save=true)
#         beta=coef(results)
#         push!(β, beta[2])
#         push!(β_mktrf, beta[2]*temp[end, :mktrf])
#         push!(s, beta[3])
#         push!(s_smb, beta[3]*temp[end, :SMB])
#         push!(h, beta[4])
#         push!(h_hml, beta[4]*temp[end, :HML])
#         push!(r, beta[5])
#         push!(r_rmw, beta[5]*temp[end, :RMW])
#         push!(c, beta[6])
#         push!(c_cma, beta[6]*temp[end, :CMA])
#         push!(m, beta[7])
#         push!(m_mom, beta[7]*temp[end, :Mom])
#         ϵ=residuals(results, temp)
#         push!(α_ff5fm, beta[1]+ϵ[end])

#         results=reg(temp,capm, save=true)
#         beta=coef(results)
#         push!(β_capm, beta[2])
#         push!(β_capm_mktrf, beta[2]*temp[end, :mktrf])
#         ϵ=residuals(results, temp)
#         push!(α_capm, beta[1]+ϵ[end])

#         R[t, :α]=beta[1]+ϵ[end]

#     end

#      # Rolling volatility
#     rVol=[100*std(R[(i-w+1):i, :ret]) for i in w:size(R[!, :ret])[1]]
#     rVolBenchmark=[100*std(R[(i-w+1):i, :benchmark]) for i in w:size(R[!, :benchmark])[1]]
 
#     plot()
#     plot!(t_day, rVol, label="Volatility")
#     plot!(t_day, rVolBenchmark, label="Volatility Market")
#     ylabel!("Daily return %")
#     title!("$w days rolling volatility")
#     savefig("$(f)rvolatility$ns")
    
#     plot()
#     plot!(t_day, β_capm, label=L"\beta_{1F}", legend=:bottomleft)
#     plot!(t_day, β, label=L"\beta_{5F}", legend=:bottomleft,)
#     ylabel!(L"\beta ")
#     title!("$w days rolling Market exposure")
#     savefig("$(f)marketExposure$ns")
#     plot()
#     plot!(t_day, s, label="Size", legend=:bottomleft)
#     plot!(t_day, h, label="Value", legend=:bottomleft)
#     ylabel!(L"\beta")
#     title!("$w days rolling Size and Value exposure")
#     savefig("$(f)sizevalue$ns")

#     plot()
#     plot!(t_day, r, label="Profitability", legend=:bottomleft)
#     plot!(t_day, c, label="Investment", legend=:bottomleft)
#     ylabel!(L"\beta")
#     title!("$w days rolling Profitability and Investment exposure")
#     savefig("$(f)profinv$ns")

#     plot()
#     plot!(t_day, m, label="Momentum", legend=:bottomleft)
#     ylabel!(L"\beta")
#     title!("$w days rolling Momentum exposure")
#     savefig("$(f)momentum$ns")

#     plot()
#     plot!(t_day, α_capm, label=L"\hat{\alpha}+\epsilon", legend=:bottomleft)
#     plot!(t_day, β_mktrf, label=L"\hat{\beta} \times mktrf", legend=:bottomleft)
#     hline!([mean(α_capm)], label=L"E[\alpha] = %$(round(mean(α_capm), digits=2))")
#     hline!([mean(R[w:end, :ret]*100)], label=L"E[r] = %$(round(mean(R[w:end, :ret]*100), digits=2))")
#     ylabel!("Daily return %")
#     title!("$w days rolling return decomposition CAPM")
#     savefig("$(f)returns$ns")

#     #Alpha month by month
#     function t_month(x)
#         Dates.format(x, "u-yy")
#     end
#     months=unique(t_month.(t_day))


#     R[!, :month]=t_month.(R[!, :t_day])
#     avg_α=[]
#     for mm in months
#         temp=R[(R[!, :month] .== mm) .& (R[!, :α] .!= -99.0), :α]
#         push!(avg_α, mean(temp))
#     end

#     plot()
#     bar!(months, avg_α, label=L"\hat{\alpha}+\epsilon", 
#          legend=:topleft, xrotation=45)
#     title!("Monthly average abnormal return")
#     ylabel!("Daily Return %")
#     savefig("$(f)alphas$ns")

#     avg_r=[]
#     for mm in months
#         temp=R[(R[!, :month] .== mm) .& (R[!, :ret] .!= -99.0), :ret]
#         push!(avg_r, mean(temp)*100)
#     end

#     plot()
#     bar!(months, avg_r, label=L"E[r_t]", 
#          legend=:topleft, xrotation=45)
#     title!("Monthly average daily return")
#     ylabel!("Daily Return %")
#     savefig("$(f)monthlyReturns$ns")

   
# end

# # function holdingHorizons(min_date, max_date, benchmark, returns, ns, f)
# #     # First, if holding the strategy 1 day, 1 week, ... in what % of time I obtain 
# #     # positive returns, in what % of time I beat the benchmark?
# #     min_year=Dates.year(min_date)
# #     max_year=Dates.year(max_date)
# #     time_span=min_date:Day(1):max_date
  

# #     # For testing purposes
# #     # returns=0.01 .+0.01 .*rand(Normal(),365*3)
# #     # benchmark=0.005 .+0.01 .*rand(Normal(),365*3)
# #     beatsMarket=[]
# #     beats=[]

# #     marketVolatility=[]
# #     returnVolatility=[]
# #     # @showprogress for h in 0:365*2 #(size(returns)[1]-1)
       
# #     #     # We compute the cumulative return
# #     #     cum_ret      = [prod(1+returns[i]   for i=j:(j+h)) for j=1:(size(returns)[1]-h)]
# #     #     cum_benchmark= [prod(1+benchmark[i] for i=j:(j+h)) for j=1:(size(benchmark)[1]-h)]

# #     #     dailyEquivalent=[cum_ret[i]^(1/(1+h)) for i=1:(size(returns)[1]-h)] .- 1.0
# #     #     dailyEquivalentB=[cum_benchmark[i]^(1/(1+h)) for i=1:(size(returns)[1]-h)] .- 1.0
# #     #     push!(returnVolatility, round(100*std(dailyEquivalent),digits=2))
# #     #     push!(marketVolatility, round(100*std(dailyEquivalentB), digits=2))

# #     #     # What % beats the Market or has positive returns
# #     #     push!(beatsMarket, round(100*sum(cum_ret .> cum_benchmark)/size(cum_ret)[1],digits=2))
# #     #     push!(beats, round(100*sum(cum_ret .> 1.0)/size(cum_ret)[1],digits=2))
        
# #     # end

# #     # horizon=collect(0:(size(beats)[1]-1))
# #     # plot()
# #     # plot!(horizon, beatsMarket, label="Beats the market")
# #     # plot!(horizon, beats, label="Positive return")
# #     # xlabel!("Holding horizon")
# #     # ylabel!(" % of time")
# #     # vline!([7, 30, 30*6, 365], label="")
# #     # plot!(xticks=([7, 30, 30*6, 365], ["1w", "1m", "6m", "1y"]))
# #     # plot!(xrotation=90 )
# #     # savefig("$(f)horizons$ns")

# #     # horizon2=horizon[1:252]
# #     # plot()
# #     # plot!(horizon2, returnVolatility[1:252], label="Avg. Vol Strategy")
# #     # plot!(horizon2, marketVolatility[1:252], label="Avg. Vol Benchmark")
# #     # xlabel!("Holding horizon")
# #     # ylabel!("Daily Volatility")
# #     # plot!(xrotation=90 )
# #     # plot!(xticks=([7, 30, 30*6], ["1w", "1m", "6m"]))
# #     # savefig("$(f)volatility$ns")
# # end

# function monthlyReturns(time_span, ret_benchmark, ret_portfolio, f, time_span_vix, vix_monthly)

#     # Unique months
#     function t_month(x)
#         Dates.format(x, "u-yy")
#     end
#     months=unique(t_month.(time_span))

#     mret=[]
#     mretb=[]
#     @showprogress for m in months
#         #Returns for that month
#         temp_ret=ret_portfolio[t_month.(time_span) .== m]
#         temp_b=ret_benchmark[t_month.(time_span) .== m]

#         # Realized return 
#         rret=round((prod(1+temp_ret[i] for i=1:size(temp_ret)[1])-1)*100, digits=2)
#         push!(mret, rret)
#         rben=round((prod(1+temp_b[i] for i=1:size(temp_ret)[1])-1)*100, digits=2)
#         push!(mretb, rben)
#     end

#     plot()
#     bar!(months, mret, 
#     legend=:topleft, xrotation=45, label="")
#     ylabel!("Return % first to last day of month")
#     hline!([mean(mret)], label="Average $(round(mean(mret), digits=2)) %")
#     hline!([std(mret)], label="Std $(round(std(mret), digits=2)) %")

#     plot!(twinx(), time_span_vix, vix_monthly, label="VIX",  xrotation=45)
#     savefig("$(f)monthly")
    
#     plot()
#     bar!(months, mretb, 
#     legend=:topleft, xrotation=45, label="")
#     ylabel!("S&P500 Return % first to last day of month")
#     hline!([mean(mretb)], label="Average $(round(mean(mretb), digits=2)) %")
#     hline!([std(mretb)], label="Std $(round(std(mretb), digits=2)) %")
#     savefig("$(f)monthlyb")

#     #Performance 2021
#     # @showprogress for y in unique(year.(time_span))
#     #     cum2020=[prod(1+ret_portfolio[year.(time_span) .== y][i] for i=1:j) for j=1:size(ret_portfolio[year.(time_span) .== y])[1]]
#     #     cumb2020=[prod(1+ret_benchmark[year.(time_span) .== y][i] for i=1:j) for j=1:size(ret_benchmark[year.(time_span) .== y])[1]]
#     #     time2020=time_span[year.(time_span) .== y]
#     #     plot()
#     #     plot!(time2020, cum2020, label="Portfolio", legend=:topleft)
#     #     plot!(time2020, cumb2020, label="S&P500")
#     #     savefig("$(f)backtest$y")
#     # end

# end



