#--------------------------------------------------------
# PROGRAM NAME - CompareStrategies.jl
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.1    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGen
#
# USAGE - Compares two strategies and produces a detailed report
#
# REQUIRES - Julia
#
# SYSTEM - Any
#
# DATE - Apr 13 2021  (Tue) 19:07
#
# BUGS - Not known
#	
#
# DESCRIPTION - 
#			
#			
#--------------------p=E[mx]------------------------------

using CSV
using DataFrames
using Dates
using Statistics
using HTTP
using Pickle
using GLM

include("analysis/Tools.jl")
#include("analysis/Simulation.jl")

function nasdaqInformation(df)
    # Adds Nasdaq
    date_to_unix(x::Date)=convert(Int64, floor(datetime2unix(DateTime(x))))
    τ=date_to_unix(today())
    t₀=date_to_unix(today()-Year(20))
    url="https://query1.finance.yahoo.com/v7/finance/download/%5EIXIC?period1=$t₀&period2=$τ&interval=1d&events=history&includeAdjustedClose=true"
    res=HTTP.get(url)
    nasdaq_df = CSV.read(res.body, DataFrame)
    computeReturnColumn!(nasdaq_df) # Adds a return column
    rename!(nasdaq_df, Dict(:Date => :time_span))
    rename!(nasdaq_df, Dict(:ret => :ret_nasdaq))
    rename!(nasdaq_df, Dict(Symbol("Adj Close") => :nasdaq))
    return innerjoin(df, nasdaq_df[!, [:time_span, :ret_nasdaq, :nasdaq]], on=[:time_span]) # Modifies dataframe
end

function sp500Information(df)
    # Adds sp500
    date_to_unix(x::Date)=convert(Int64, floor(datetime2unix(DateTime(x))))
    τ=date_to_unix(today())
    t₀=date_to_unix(today()-Year(20))
    url="https://query1.finance.yahoo.com/v7/finance/download/%5EGSPC?period1=$t₀&period2=$τ&interval=1d&events=history&includeAdjustedClose=true"
    res=HTTP.get(url)
    sp500_df = CSV.read(res.body, DataFrame)
    computeReturnColumn!(sp500_df) # Adds a return column
    rename!(sp500_df, Dict(:Date => :time_span))
    rename!(sp500_df, Dict(:ret => :ret_benchmark))
    rename!(sp500_df, Dict(Symbol("Adj Close") => :sp500))
    return innerjoin(df, sp500_df[!, [:time_span, :ret_benchmark, :sp500]], on=[:time_span]) # Modifies dataframe
end


function riskFreeInformation(df)
    date_to_unix(x::Date)=convert(Int64, floor(datetime2unix(DateTime(x))))
    τ=date_to_unix(today())
    t₀=date_to_unix(today()-Year(20))
    url="https://query1.finance.yahoo.com/v7/finance/download/%5EIRX?period1=$t₀&period2=$τ&interval=1d&events=history&includeAdjustedClose=true"
    res=HTTP.get(url)
    rf_df = CSV.read(res.body, DataFrame)
    rename!(rf_df, Dict(:Date => :time_span))
    rename!(rf_df, Dict(Symbol("Adj Close") => :rf))

    rf_df.rf = map(x->begin val = tryparse(Float64, x)
                           ifelse(typeof(val) == String, missing, val)
                      end, rf_df.rf)
    rf_df.rf=replace(rf_df.rf, nothing => missing)
    dropmissing!(rf_df)
    rf_df.rf=rf_df.rf ./ (100*13*7) # 13 week Treasury Bill
    return innerjoin(df, rf_df[!, [:time_span, :rf]], on=[:time_span]) # Modifies dataframe
end

#**********************************
# First strategy
#**********************************
s1="TRPL"
csv_file="data/backtest$(s1).csv"
df=CSV.read(csv_file, DataFrame)
df= nasdaqInformation(df) # Adds nasdaq info            
df= sp500Information(df)  # Adds sp500 info
df= riskFreeInformation(df) # Adds risk free info
df=df[completecases(df), :]
ret_portfolio=df.ret_portfolio
time_span=df.time_span
rf=df.rf
names_portfolios=["", "Portfolio", "S&P500", "Nasdaq"] # Keep the first one empty
benchmarks=df[:, ["ret_benchmark", "ret_nasdaq"]]

#**********************************
# Second strategy
#**********************************
s2="TRPF"
csv_file="data/backtest$(s2).csv"
df2=CSV.read(csv_file, DataFrame)
rename!(df,  Dict(Symbol("ret_portfolio") => Symbol("ret_portfolio$(s1)")))
rename!(df2, Dict(Symbol("ret_portfolio") => Symbol("ret_portfolio$(s2)")))
df=innerjoin(df, df2, on=[:time_span])


# Good and bad times with the sp500
peak=[maximum(df.sp500[1:i]) for i =1:size(df.sp500)[1]]

df.bear=[ (df.sp500[i]/peak[i] < 0.8) & (df.ret[i]<0) for i =1:size(df.sp500)[1]]

plot()
for por in ["TRPF", "TRPL"]
    df[!, "cumret_portfolio$(por)"]=[ prod(1+df[i,"ret_portfolio$(por)"] for i=1:j) for j=1:size(df.time_span)[1]]
    plot!(df.time_span, df[!, "cumret_portfolio$(por)"], label=por)
end
plot!(title="1 USD Invested between $(minimum(df.time_span)) - $(maximum(df.time_span))", legend=:topleft)

savefig("data/performance")

linearRegressor1 = lm(@formula(ret_portfolioTRPL ~ ret_benchmark + bear), df)

linearRegressor2 = lm(@formula(ret_portfolioTRPF ~ ret_benchmark + bear), df)

linearRegressor1 = lm(@formula(ret_portfolioTRPL ~  bear), df)

linearRegressor2 = lm(@formula(ret_portfolioTRPF ~  bear), df)
