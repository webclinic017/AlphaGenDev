#--------------------------------------------------------
# PROGRAM NAME - OptimizeParameters.jl
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.1    [Mayor].[Minor].[Patch]
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
# DATE - 14-6-2021
#
# BUGS - Not known
#	
#
# DESCRIPTION - Hyperparameter optimization
#
# Log - 
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
using GLM

PATH_TO_SEC_DATA=ENV["PATH_TO_SEC_DATA"]

include("signal/Backtest.jl")


function objectiveFunction(cum_ret, cum_benchmark)
    if size(cum_ret) != size(cum_benchmark)
        @error("Vectors do not have the same size")
    else
        return cum_ret[end]-cum_benchmark[end]
    end
end

# How to encode solutions?
# percentile liquid + minprice + DD_tile
# liquidid 90, 95, 99 or -1 0 1
# minprice 1,2,5      or -1 0 1
# DD_tile 1,5,10      or -1 0 1
# t_signal xb or xb+fe   0 1

function translateSolution(x)

# x e.g. x=[-1 0 0]
    pliq=0.0
    if x[1] == -1
        pliq=90
    elseif x[1] == 0
        pliq=95
    elseif x[1] == 1
        pliq=99
    end

    mprice=0.0
    if x[2] == -1
        mprice=1.0
    elseif x[2] == 0
        mprice=2.0
    elseif x[2] == 1
        mprice=5.0
    end

    DD_tile=0.0
    if x[3] == -1
        DD_tile=1
    elseif x[3] == 0
        DD_tile=5
    elseif x[3] == 1
        DD_tile=10
    end

    t_sig=""
    if x[4] == 1
        t_sig="xb"
    elseif x[4] == 0
        t_sig="xb+fe"
    end

    return pliq, mprice, DD_tile, t_sig
end

function neighbor(x)

    # Pick a dimension to modify 1,2,3
    xp=copy(x)
    d=rand((1,2,3,4))

    #Given the dimension if encoding is -1 or 1 we move to 0, otherwise 50-50
    if d<=3
        if xp[d] == -1
            xp[d]=0
        elseif xp[d]==1
            xp[d]=0
        else
            xp[d]= rand()<0.5 ? 1 : -1
        end
    else
        xp[d]= xp[d] == 1 ? 0 : 1
    end
    return xp
end

function simulatedAnnealing(nl, ns; kmax=10, T=2)
    solution_backtests=[]
    solutions=[]

    min_date=Dates.Date(2010,1,1)
    max_date=Dates.Date(2021,5,20)
    time_span=min_date:Day(1):max_date # Iterator of dates
    time_span=[t for t in time_span if dayofweek(t)==4]

    x = [0 0 0 0]
    pliq, mprice, DD_tile, t_sig = translateSolution(x)

    time, ret_portfolio, cum_ret, cum_benchmark, TICKERS, WEIGHTS, nlongs, nshorts, ret_benchmark, problematic= generate_backtest(min_date, 
    max_date, time_span, frequency="w", verbose=false,  type_signal=t_sig, nl=nl, ns=ns, pliquid=pliq, minprice=mprice, DD_tile=DD_tile);

    obj =  - objectiveFunction(cum_ret, cum_benchmark)
    for k=0:(kmax-1)
        println("Iteration $k objective function $(-obj) and solution $(x)")
       # Checks the obj function at the neighbor
        xp=neighbor(x)
        pliq, mprice, DD_tile, t_sig = translateSolution(xp)
        time, ret_portfolio, cum_ret, cum_benchmark, TICKERS, WEIGHTS, nlongs, nshorts, ret_benchmark, problematic= generate_backtest(min_date, 
        max_date, time_span, frequency="w", verbose=false,  type_signal=t_sig, nl=nl, ns=ns, pliquid=pliq, minprice=mprice, DD_tile=DD_tile);
        nobj =  - objectiveFunction(cum_ret, cum_benchmark)
        
        temp = T /(k +1)
        if nobj < obj
            x=xp
            obj=nobj
            push!(solution_backtests, ret_portfolio)
            push!(solutions, x)
        else
            if rand() < exp(-(nobj-obj)/temp)
                println("prob $(exp(-(nobj-obj)/temp))")
                x=xp
                obj=nobj
                push!(solution_backtests, ret_portfolio)
                push!(solutions, x)
            end
        end

    end

    return x, solution_backtests, solutions
end


x, solution_backtests, solutions=simulatedAnnealing(25,25)

plot(solution_backtests[1])