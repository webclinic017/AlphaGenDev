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
using GLM
using ShiftedArrays

PATH_TO_SEC_DATA=ENV["PATH_TO_SEC_DATA"]

# Any preditive model comes hand in hand with a backtest that can go eithr long or short on the index

min_date=Dates.Date(2010,1,1)
max_date=Dates.Date(2021,4,15)
time_span=min_date:Day(1):max_date # Iterator of dates
Frets=[]
RFrets=[]
benchmark=CSV.read("$(PATH_TO_SEC_DATA)\\yahoo_finance\\data_etfs\\^GSPC.csv", DataFrame)
ret_strategy=[]
ret_benchmark=[]
time=[]
Fret=0.0
Fret_ind=[]
last_ret=0.0

last_ret_ind=[]

etfs=["XLB", "XLI", "XLY", "XLP", "XLE", "XLF", "XLU", "XLV", "XLK"]

names_industry=Dict()
names_industry["XLB"] = "Materials"
names_industry["XLI"] = "Industrials" 
names_industry["XLY"] = "Consumer Discretionary"
names_industry["XLP"] = "Consumer Staples"
names_industry["XLE"] = "Energy"
names_industry["XLF"] = "Financials"
names_industry["XLU"] = "Utilities"
names_industry["XLV"] = "Health Care"
names_industry["XLK"] = "Information Technology"

ret_industries=Dict()
ret_industries_strategy=Dict()
re_industries=Dict()
Fre_industries=Dict()
for etf in etfs
    ret_industries[etf]=[]
    ret_industries_strategy[etf]=[]
    re_industries[etf]=[]
    Fre_industries[etf]=[]
    push!(Fret_ind, 0.0)
    push!(last_ret_ind, 0.0)
end
etfs_data=[CSV.read("$(PATH_TO_SEC_DATA)\\yahoo_finance\\data\\$(etf).csv", DataFrame) for etf in etfs]
for i in 1:size(etfs_data)[1]
    temp=etfs_data[i]
    temp.t_day=temp.date
    etfs_data[i]=temp
end
@showprogress for t in time_span
    
    
    # First check, is it a trading day for the sp500?
    sp500 = @from i in benchmark begin
        @where i.t_day == t
        @select {i.ret, i.t_day}
        @collect DataFrame
    end

    industries=[@from i in etf_d begin
        @where i.t_day == t
        @select {i.ret, i.t_day}
        @collect DataFrame
    end for etf_d in etfs_data]

    if size(sp500)[1]==0
        continue
    end

    push!(ret_benchmark, sp500.ret[1])

    for i in 1:size(etfs)[1]
        temp=ret_industries[etfs[i]]
        push!(temp, industries[i].ret[1])
        ret_industries[etfs[i]]=temp

    end

    # Long or short?
    yyyy=year(t)
    mm=month(t)
    dd=day(t)
    try
    sg= DataFrame(load("$(PATH_TO_SEC_DATA)\\signals\\v1\\sp500$yyyy-$mm-$dd.dta"))
    Fret = sg.Fret_sp500[1]
    catch
    end

    # Long or short on the industries
    try
        sgind= DataFrame(load("$(PATH_TO_SEC_DATA)\\signals\\v1\\etfs$yyyy-$mm-$dd.dta"))
      

        Fret_ind[1]=sgind.Fret_Materials[1]
        Fret_ind[2]=sgind.Fret_Industrials[1]
        Fret_ind[3]=sgind.Fret_ConsumerDiscretionary[1]
        Fret_ind[4]=sgind.Fret_ConsumerStaples[1]
        Fret_ind[5]=sgind.Fret_Energy[1]
        Fret_ind[6]=sgind.Fret_Financials[1]
        Fret_ind[7]=sgind.Fret_Utilities[1]
        Fret_ind[8]=sgind.Fret_HealthCare[1]
        Fret_ind[9]=sgind.Fret_InformationTechnology[1]

    catch
    end

    if size(Frets)[1] > 0
        if Fret >= 0.01 #percentile(Frets, 10)
            push!(ret_strategy, sp500.ret[1])
        elseif Fret <= -0.01
            push!(ret_strategy, - sp500.ret[1])
        else
            push!(ret_strategy, 0.0)
        end
    else
        push!(ret_strategy, sp500.ret[1])
    end

   
    last_ret=sp500.ret[1]
    push!(time, t)

    push!(Frets, Fret)


    for i=1:size(etfs)[1]
        temp=ret_industries_strategy[etfs[i]]
        #!TODO conviction, use another threshold
        if Fret_ind[i]>=0.01
            push!(temp, ret_industries[etfs[i]][end])
        elseif Fret_ind[i]<=-0.01
            push!(temp, -ret_industries[etfs[i]][end])
        else
            push!(temp, 0.0)
        end
        ret_industries_strategy[etfs[i]]=temp

        temp=Fre_industries[etfs[i]]
        push!(temp, Fret_ind[i])
        Fre_industries[etfs[i]]=temp

    end


    # Then we need to compare Fret with the realized return of the benchmark
    re_sp500 = @from i in benchmark begin
        @where t <= i.t_day <= t + Day(30)
        @select {i.adjclose, i.t_day}
        @collect DataFrame
    end

    Rrets=lead(re_sp500.adjclose, 15) ./ re_sp500.adjclose .- 1.0
    Rret=Rrets[1]
    push!(RFrets, Rret)

    re_ind = [@from i in etf_d begin
        @where t <= i.t_day <= t + Day(30)
        @select {i.adjclose, i.t_day}
        @collect DataFrame
    end for etf_d in etfs_data]

    for i in 1:size(etfs)[1]
        Rrets=lead(re_ind[i].adjclose, 15) ./ re_ind[i].adjclose .- 1.0
        Rret=Rrets[1]
        temp=re_industries[etfs[i]]
        push!(temp, Rret)
        re_industries[etfs[i]]=temp
    end

end

# Cum rets
cum_strategy =[prod(1+ret_strategy[i]  for i=1:j) for j=1:size(ret_strategy)[1]]
cum_benchmark=[prod(1+ret_benchmark[i] for i=1:j) for j=1:size(ret_benchmark)[1]]

cum_industries_strategies=[[prod(1+ret_industries_strategy[e][i]  for i=1:j) for j=1:size(ret_industries_strategy[e])[1]] for e in etfs]
cum_industries=[[prod(1+ret_industries[e][i]  for i=1:j) for j=1:size(ret_industries[e])[1]] for e in etfs]


i=5
plot()
plot!(time, cum_industries_strategies[i], label= "$(names_industry[etfs[i]]) Strategy")
plot!(time, cum_industries[i], label= "$(names_industry[etfs[i]]) $(etfs[i]) ETF")
plot!(legend = :topleft)

plot()
plot!(time, cum_strategy, label="Index Strategy")
plot!(time, cum_benchmark, label="SP500")
plot!(legend= :topleft)

# # # #Of times that we manage to predict the sign
# sign_acc=round(100*sum( sign.(Frets) .== sign.(RFrets))/size(Frets)[1], digits=2)
# @info("Sign accuracy $sign_acc %")



# sign_accs=[round(100*sum( sign.(Fre_industries[e]) .== sign.(re_industries[e]))/size(Fre_industries[e])[1], digits=2) for e in etfs]

# for i=1:size(etfs)[1]
#     @info("Sign accuracy $(names_industry[etfs[i]]) => $(sign_accs[i]) %")
# end

# plot()
# plot!(time, Frets, label="Predicted")
# plot!(time, RFrets, label="Realized")

# plot()
# plot!(RFrets, Frets,  seriestype = :scatter, smooth=true)
# ylabel!("Forecasted")
# xlabel!("Realized")
# plot()
# plot!(time, ret_strategy-ret_benchmark)






