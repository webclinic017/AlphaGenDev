#--------------------------------------------------------
# PROGRAM NAME - PortfolioManagement.jl 
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.2    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGen Investment Strategies
#
# USAGE - Implements the optimization procedures
#
# REQUIRES - Expected Returns, Variances, and Optimization 
#            Parameters
#
# SYSTEM - All, but requires an existing CPLEX or Gurobi installation
#
# DATE - Mar 27 2021  (Sat) 17:53
#
# BUGS - Not known
#	
#
# DESCRIPTION - The basic optimization procedure maximizes a mean variance utility
#               given constraints on exposure, market neutrality, etc...
#
# Past information
# 
#-----------------------------------------------------------------------------
# Name: PortfolioManagement
# Date: 12/06/2020
# Description: Contains the main functions for the backtesting outside 
#              Quantopian Inc.
#
# Requires: Installation of CPLEX - versions 2.8 or 2.9
#           set environmental variable
#           ENV["CPLEX_STUDIO_BINARIES"]
#
# Changes:  Removed function custom_optimize and replace it with function
#           rebalance_optimization
#
#           The free version of CPLEX does not always take into account all the variables
#           the reason we have not had much problems is because many problems are solved at pre-processing time. 
#           
#-----------------------------------------------------------------------------
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
using JuMP
using StatsFuns
using Gurobi

ENV["GUROBI_HOME"] = "C:\\gurobi911\\win64"

"""
    bounds_on_factors(loadings::Dict, factor_position::Dict)

Creates by default lower and upper bounds on factor loadings

# Arguments

`loadings` : A dictionary of the form `Dict(Symbol(factor::String) => [lb::Float64, up::Float64])'
where lb and ub are the lower and upper bound respectively. E.g

`loadings` = `Dict(:mktrf => [-1.0, 1.0])'

`factor_position` : A dictionary that maps each factor with a row in the matrix B in expression Bω

e.g. `factor_position=Dict(:smb => 2)'
# Returns
`loads' : `Dict(Symbol(factor:string) => (lb, up, factor_position[Symbol(factor:string)]))'
"""
function bounds_on_factors(loadings::Dict, factor_position::Dict)
    loads=Dict()
    for (key, value) in loadings
        # Upper and lower bounds on each factor
        loads[key]=(value[1], # Lower bound
                     value[2], # Upper bound
                     factor_position[key]) # Row in the matrix B in B ω
    end
    return loads
end


function decile_portfolios(α)
    # Top and Bottom decile
    q=quantile(α, [0.1, 0.9])
    #  return ω_value, ωc_value, ω_long_value, ω_short_value, var
    N=size(α)[1]
    # Equally weighted long minus short
    ω_long_value=zeros(Float64, N)
    ω_short_value=zeros(Float64, N)
    ω_value=zeros(Float64, N)
    # equally weighted n the Top
    total_long=sum(α.>q[2])
    ω_long_value[α.>q[2]].=1/total_long
    total_short=sum(α.<q[1])
    ω_short_value[α.<q[2]].=1/total_short

    ω_value=ω_long_value-ω_short_value
    # In case they dont add to 1
    ω_value=ω_value./sum(ω_value)

    # Reupdates the original ones
    for i=1:N
        if ω_value[i]>0.0
            ω_long_value[i]=ω_value[i]
        else
            ω_short_value[i]=-ω_value[i]
        end
    end

    # Computes VaR
    prob_var=0.05
    z_α=norminvcdf(prob_var)
    var=sqrt( (z_α^2)*sum(ω_value[i]*Σ[i,j]*ω_value[j] for i=1:N, j=1:N))


    ωc_value=1.0 - sum(ω_value)
    return ω_value, ωc_value, ω_long_value, ω_short_value, var
end

function simpleLongShort(α, σ)

    df=DataFrame(α = α, σ = σ)
    df.obj=df.α ./ df.σ
    N=size(df.α)[1]
    df.ω = zeros(N)
    df.ω[df.obj .< percentile(df.obj, 100*25/N)] .= -2.0
    df.ω[df.obj .> percentile(df.obj, 100*(1-25/N))] .= 2.0
    df.y = df.ω .!= 0.0
    df.yp = df.ω .> 0.0
    df.yn = df.ω .< 0.0
    return df.ω, df.y, df.yp, df.yn
end

function simpleLong(α, σ)

    df=DataFrame(α = α, σ = σ)
    df.obj=df.α 
    N=size(df.α)[1]
    df.ω = zeros(N)
    df.ω[(df.obj .> percentile(df.obj, 100*(1-50/N))) ] .= 2.0 #& (df.σ .<= percentile(df.σ, 50))
    df.y = df.ω .!= 0.0
    df.yp = df.ω .> 0.0
    df.yn = df.ω .< 0.0
    return df.ω, df.y, df.yp, df.yn
end


function optimizationSquarePoint(α, σ, GRB_ENV; γ=5.0, only_long=false)

    model=JuMP.Model(with_optimizer(() -> Gurobi.Optimizer(GRB_ENV)))
    set_silent(model) 
    # Number of assets
    N=length(α)

    @variable(model, y[1:N], Bin)
    
    
    @variable(model, ω[1:N], Int)
    @variable(model, ω_long[1:N], Int)
    @variable(model, ω_short[1:N], Int)
   
    #Redefinition ω=ω+ - ω-
    for i=1:N
        @constraint(model, ω[i]==ω_long[i]-ω_short[i])
        @constraint(model, -4<=ω[i]<=4)
    end

    @constraint(model, sum(ω_long[i]+ω_short[i] for i=1:N)==100.0)

    @variable(model, yp[1:N], Bin)
    @variable(model, yn[1:N], Bin)

  
    M=20000000.0
    for i in 1:N
        @constraint(model, y[i]==yp[i]+yn[i])
        @constraint(model, M*ω_long[i]>=yp[i])
        @constraint(model, ω_long[i]<=M*yp[i])
        @constraint(model, M*ω_short[i]>=yn[i])
        @constraint(model, ω_short[i]<=M*yn[i])
    end

    if only_long
        
        for i=1:N
            @constraint(model, yn[i]==0.0)
        end
  
    end

    @constraint(model, sum(y[i] for i=1:N)==50.0)

    #@objective(model, Max, sum((α[i]/σ[i])*ω[i]/100 for i=1:N));
    @objective(model, Max, sum((α[i])*ω[i]/100 for i=1:N));
  
    optimize!(model)
 
    ω_value = value.(ω)
    y_value = value.(y)
    yp_value = value.(yp)
    yn_value = value.(yn)
   
    return ω_value, y_value, yp_value, yn_value
end


function optimizationRevolut(α, GRB_ENV=Gurobi.Env())

    model=JuMP.Model(with_optimizer(() -> Gurobi.Optimizer(GRB_ENV)))
    set_silent(model) 
    # Number of assets
    N=length(α)
    @variable(model, y[1:N], Bin) #Easy hold one stock strategy
    @objective(model, Max, sum(α[i]*y[i] for i=1:N));
    # Only one stock
    @constraint(model, sum(y[i] for i=1:N)==1.0)

    optimize!(model)
    return value.(y)
end

function rebalance_optimization(α, β, βn, u_up, u_down, L, S; min_position=0.00, nlong=25, nshort=25,
                                 use_sector_exposure=false, sector_exposure=0.15, βₘ=0.0, GRB_ENV=[], βnasdaq=0.0, free_longshort=false)

    #Creates the JuMP model with the Solver 
    #model=Model(with_optimizer(CPLEX.Optimizer))
    model=JuMP.Model(with_optimizer(() -> Gurobi.Optimizer(GRB_ENV)))
    # No verbose
    set_silent(model) 
    # Number of assets
    N=length(α)

    @variable(model, u_down[i] <= ω[i=1:N] <= u_up[i]) # u_down ≤ ω ≤ u_up
    @variable(model, ω_c ) # Cash

    #For leverage we need the long and short positions
    @variable(model, 0.0 <= ω_long[1:N])
    @variable(model, 0.0 <= ω_short[1:N])
    #Redefinition ω=ω+ - ω-
    for i=1:N
        @constraint(model, ω[i]==ω_long[i]-ω_short[i])
    end
    # Budget constraint
    # Turns out quantopiuan doesnt have this constraint
    #
    @constraint(model, sum(ω[i] for i=1:N)+ ω_c== 1.0)
    # ! Only for SquarePoint testing
    # Hold 50 companies # Hold 25 long and 25 short

    # Create some binary variables to see if we have a company either long or short
    @variable(model, yp[1:N], Bin)
    @variable(model, yn[1:N], Bin)
    # So, if ω is different than zero the binary variable activates

    # M*ω >= y 
    # ω <= M*y
    # If y=1 ω>0 , if y=0 ω has to be zero
    # If ω>0 y has to be 1, if ω=0 y has to be zerp
    M=20000000.0
    for i in 1:N
        @constraint(model, M*ω_long[i]>=yp[i])
        @constraint(model, ω_long[i]<=M*yp[i])
        @constraint(model, M*ω_short[i]>=yn[i])
        @constraint(model, ω_short[i]<=M*yn[i])
        @constraint(model, yp[i]*min_position<=ω_long[i])
        @constraint(model, yn[i]*min_position<=ω_short[i])
    end

    if free_longshort
        @constraint(model, sum(yp[i] + yn[i] for i=1:N)==nlong+nshort)
    else
        @constraint(model, sum(yp[i] for i=1:N)==nlong)
        @constraint(model, sum(yn[i] for i=1:N)==nshort)
    end
    
    if use_sector_exposure
        #I make sure I dont have more than some percent invested in a sector
        # How many industries
        K=size(S)[2]

        for k in 1:K
            @constraint(model, sum((yp[i]+yn[i])*S[i,k] for i=1:N)<=sum((yp[i]+yn[i]) for i=1:N)*sector_exposure)
        end

    end
    
    # Now, if a weight is positive it has to be more than x%
    
    #Factor loading constraints
    #@constraint(model, -0.05 <= sum(β[i]*ω[i] for i=1:N) <= 0.05)
    if βₘ!=-99
        @constraint(model,  sum(β[i]*ω[i] for i=1:N) == βₘ)
    end

    if βnasdaq!=-99
        @constraint(model,  sum(βn[i]*ω[i] for i=1:N) <= βnasdaq+0.5)
        @constraint(model,  sum(βn[i]*ω[i] for i=1:N) >= βnasdaq-0.5)
    end

    # For leverage we use the split definition and create a linear constraint

    #@constraint(model, sum(ω_long[i] for i=1:N) + sum(ω_short[i] for i=1:N)<=1.0)
    #
    @constraint(model,sum(ω_long[i] for i=1:N) +sum(ω_short[i] for i=1:N)<=L*(sum(ω_long[i] for i=1:N) - sum(ω_short[i] for i=1:N)+ω_c))

    #@constraint(model, (z_α^2)*sum(ω[i]*Σ[i,j]*ω[j] for i=1:N, j=1:N)<=(MADD-cD)^2);
    # Objective
    @objective(model, Max, sum(α[i]*ω[i] for i=1:N));
  
    optimize!(model)
 
    ω_value = value.(ω)
    ωc_value = value.(ω_c)
    ω_long_value=value.(ω_long)
    ω_short_value=value.(ω_short)
    ω_value=ω_value/sum(abs.(ω_value))
    ω_long_value=ω_long_value/sum(abs.(ω_value))
    ω_short_value=ω_short_value/sum(abs.(ω_value))

    return ω_value, ωc_value, ω_long_value, ω_short_value

 end