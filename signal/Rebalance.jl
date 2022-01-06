
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
t=Date(2022, 1, 6)
y=year(t)
information_set=CSV.read("$(PATH_TO_SEC_DATA)\\information_set$y.csv", DataFrame)
information_set2=CSV.read("$(PATH_TO_SEC_DATA)\\information_set$(y-1).csv", DataFrame)
df=@from i in information_set begin
    @where  i.t_day < t #! NOTE, THE STRICT < SIGN
    @select {i.t_day, i.open, i.adjclose, i.volume, i.ticker, i.ret, i.cshoq, i.sic, i.beta}
    @collect DataFrame                
end


df_a=@from i in information_set2 begin
    @select {i.t_day, i.open, i.adjclose, i.volume, i.ticker, i.ret, i.cshoq, i.sic, i.beta}
    @collect DataFrame     
end

df= append!(df, df_a) 


maxt=maximum(df.t_day)
#println(t)
@assert t>maxt # Makse sure we add the one before and not after
#We keep the last month of observations to compue a measure of liquidity

df2=@from i in df begin
    @where (maxt-Month(3)<=i.t_day<=maxt) & !isnan(i.volume) & !isnan(i.adjclose) &!isnan(i.ret)
    @select {i.ticker,i.t_day, i.open, i.adjclose, i.volume, i.ret, i.cshoq, i.sic, i.beta}
    @collect DataFrame        
end

sort!(df2, [order(:ticker), order(:t_day)])
gdf=groupby(df2, [:ticker])
df_combined2=combine(gdf, :adjclose => minimum, :adjclose => maximum, :adjclose => last, :adjclose => first)
df_combined2[!, "DD"]=df_combined2.adjclose_last ./df_combined2.adjclose_maximum .- 1.0
df_combined2[!, "DU"]=df_combined2.adjclose_maximum ./df_combined2.adjclose_last.- 1.0
df_combined2 = df_combined2[: , ["ticker", "DD", "DU"]]


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
df_combined=combine(gdf, :illiq => mean, :volume => minimum, :me => last, :beta => last, :sic => last, :adjclose => last, :volume=> last,
                    :adjclose => minimum)

# Merge with the signal data before removing highly illiquid stocks
# this is because some of them have been filtered already by only estimating the model
# e.g. with stocks with prc >=5

# What is my signal? Either signalt.dta or the closest one
# Finds the closest file from above of time t

date_signal=t
yyyy=year(date_signal)
mm=month(date_signal)
dd=day(date_signal)
signal=DataFrame(load("$(PATH_TO_SEC_DATA)\\signals\\v1\\signal$yyyy-$mm-$dd.dta"))

signal=signal[!, [:ticker, :Fret, :Eret, :fe, :sd_resid]]

df=innerjoin(signal, df_combined, on = :ticker)
df=innerjoin(df, df_combined2, on = :ticker)
sort!(df, [order(:ticker)])
df=df[df.DD .>= percentile(df.DD, 5), :]
df=df[df.DU .<= percentile(df.DU, 95), :]
df[df.ticker .== "acnd", :]

#.* (!).(isnan.(df.illiq_mean))
df=df[completecases(df) , :]

#df=df[df.sd_resid .> 0.0, :]
# Make sure we also have info on σ\_ϵ   
#println(t)
df=df[(!).(isnan.(df.illiq_mean)) .* (!).(isnan.(df.me_last)) , :]
#df=df[df.me_last .<= percentile(df.me_last, 90), :]
df=df[df.illiq_mean .<= percentile(df.illiq_mean, 95), :] #!!!!!!!!!! 
df=df[df.ticker .!= "nspr", :]
# Let's put the restriction to pennys tocks in the universe
df=df[(!).(ismissing.(df.adjclose_last)), :]
df=df[df.adjclose_last .>= 2.0 , :] #!!!!!!!!!! 
df=df[df.volume_last .> 0 , :]

# Filter to avoid the spikes product of retail investors

#df=df[df.sd_resid .<= percentile(df.sd_resid, 90), :]
#df=df[df.illiq_mean .<= percentile(df.illiq_mean, 50), :]
#!df=df[df.volume_minimum .> 0.0, :] # We also drop stocks with very low dollar volume

# Now we need one digit sic codes

# Returns on time t #! I should check expost if trades are done when volume is 0

# If the frequency is weekly, then before I rebalance I need to compute what has been the return of the portfolio

#! STOP HERE and compute the portfolio return before rebalancing
# ! I cannot look for stocks after filtering, maybe they got out of the sample

type_signal="xb"

if type_signal=="xb"
α=df.Eret #!
elseif type_signal=="xb+fe"
α=df.Fret
end
σ=df.sd_resid
df.one_sic=convert.(Int64, floor.(df.sic_last ./ 1000))
β=df.beta_last
S=cate_to_mat(df.one_sic)
nLong=25
nShort=25
#S=[]
env=Gurobi.Env()
ω_value, y_value, yp_value, yn_value= optimizationSquarePoint(α , σ, S, β, nLong, nShort, env, 0.15)


# Here we do a simpler version, 50 long and 50 short sorted
tick = df.ticker
tick_neg = tick[sortperm(α)]
tick_pos = tick[sortperm(α, rev=true)]
alpha_neg = α[sortperm(α)]
alpha_pos = α[sortperm(α, rev=true)]

df2 = DataFrame(tick_neg = tick_neg, alpha_neg = alpha_neg, tick_pos = tick_pos, alpha_pos = alpha_pos)
CSV.write("$(PATH_TO_SEC_DATA)\\rebalance\\$(t)_.csv", df2)
#ω_value, y_value, yp_value, yn_value= simpleLongShort(α , σ)
# #**************************************************************************************
# ub=1/10
# lb=-1/10

# use_sector_exposure=false
# βₘ=0.0

# N=size(α)[1]
# u_up=ones(N,1)*ub
# u_down= ones(N,1)*lb
# use_signal=1
# GRB_ENV=env
# free_longshort=false
# βn=[]
# #println(t)

# L=2.0

# # end
# ω_value, ωc_value, ω_long_value, ω_short_value, y_value = rebalance_optimization(α ,    #! 
#                                                                         β, 
#                                                                         βn,
#                                                                         u_up, 
#                                                                         u_down, 
#                                                                         L,
#                                                                         S,
#                                                                         GRB_ENV=GRB_ENV,
#                                                                         use_sector_exposure=use_sector_exposure, 
#                                                                         βₘ=βₘ, 
#                                                                         βnasdaq=-99,
#                                                                         nlong=nLong,
#                                                                         nshort=nShort,
#                                                                         free_longshort=free_longshort)

# ω_value .= ω_value .* 100 

#**************************************************************************************

tickers=df.ticker[y_value .≈ 1]
sig=α[y_value .≈ 1]
Fret = df.Fret[y_value .≈ 1]
prices=df.adjclose_last[y_value .≈ 1 ]
weights=ω_value[y_value .≈ 1]
y_var=y_value

@assert (sum(y_value) ≈ 50 ) 
@assert (size(tickers)[1]≈ 50)

df_bought=DataFrame(ω=weights, ticker=tickers, price=prices, sig=sig, Fret = Fret)

# Get names by ticker
ciks=CSV.read("$(PATH_TO_SEC_DATA)\\cik_ticker.csv", DataFrame)
ciks=ciks[(!).(ismissing.(ciks.ticker)), :]
df=innerjoin(df_bought, ciks, on = :ticker)

inf=CSV.read("$(PATH_TO_SEC_DATA)\\sec20211\\sub.txt", DataFrame)
inf=inf[!, [:cik, :name]]
unique!(inf)
df=leftjoin(df, inf, on = :cik)
df=df[!, [:ω, :ticker, :name, :sig, :Fret]]
CSV.write("$(PATH_TO_SEC_DATA)\\rebalance\\$t.csv", df)


