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

`factor_position` : A dictionary that maps each factor with a row in the matrix B in expression B??

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
                     factor_position[key]) # Row in the matrix B in B ??
    end
    return loads
end


function decile_portfolios(??)
    # Top and Bottom decile
    q=quantile(??, [0.1, 0.9])
    #  return ??_value, ??c_value, ??_long_value, ??_short_value, var
    N=size(??)[1]
    # Equally weighted long minus short
    ??_long_value=zeros(Float64, N)
    ??_short_value=zeros(Float64, N)
    ??_value=zeros(Float64, N)
    # equally weighted n the Top
    total_long=sum(??.>q[2])
    ??_long_value[??.>q[2]].=1/total_long
    total_short=sum(??.<q[1])
    ??_short_value[??.<q[2]].=1/total_short

    ??_value=??_long_value-??_short_value
    # In case they dont add to 1
    ??_value=??_value./sum(??_value)

    # Reupdates the original ones
    for i=1:N
        if ??_value[i]>0.0
            ??_long_value[i]=??_value[i]
        else
            ??_short_value[i]=-??_value[i]
        end
    end

    # Computes VaR
    prob_var=0.05
    z_??=norminvcdf(prob_var)
    var=sqrt( (z_??^2)*sum(??_value[i]*??[i,j]*??_value[j] for i=1:N, j=1:N))


    ??c_value=1.0 - sum(??_value)
    return ??_value, ??c_value, ??_long_value, ??_short_value, var
end

function simpleLongShort(??, ??)

    df=DataFrame(?? = ??, ?? = ??)
    df.obj=df.?? ./ df.??
    N=size(df.??)[1]
    df.?? = zeros(N)
    df.??[df.obj .< percentile(df.obj, 100*25/N)] .= -2.0
    df.??[df.obj .> percentile(df.obj, 100*(1-25/N))] .= 2.0
    df.y = df.?? .!= 0.0
    df.yp = df.?? .> 0.0
    df.yn = df.?? .< 0.0
    return df.??, df.y, df.yp, df.yn
end

function simpleLong(??, ??)

    df=DataFrame(?? = ??, ?? = ??)
    df.obj=df.?? 
    N=size(df.??)[1]
    df.?? = zeros(N)
    df.??[(df.obj .> percentile(df.obj, 100*(1-50/N))) ] .= 2.0 #& (df.?? .<= percentile(df.??, 50))
    df.y = df.?? .!= 0.0
    df.yp = df.?? .> 0.0
    df.yn = df.?? .< 0.0
    return df.??, df.y, df.yp, df.yn
end


function optimizationSquarePoint(??, ??, S, ??, nl, ns, GRB_ENV, se; ??=5.0, only_long=false)

    model=JuMP.Model(with_optimizer(() -> Gurobi.Optimizer(GRB_ENV)))
    set_silent(model) 
    # Number of assets
    N=length(??)

    @variable(model, y[1:N], Bin)
    
    
    @variable(model, ??[1:N], Int)
    @variable(model, ??_long[1:N], Int)
    @variable(model, ??_short[1:N], Int)
   
    #Redefinition ??=??+ - ??-
    for i=1:N
        @constraint(model, ??[i]==??_long[i]-??_short[i])
        @constraint(model, -2<=??[i]<=2)
    end

    @constraint(model, sum(??_long[i]+??_short[i] for i=1:N)==100.0)

    @variable(model, yp[1:N], Bin)
    @variable(model, yn[1:N], Bin)

  
    M=20000000.0
    for i in 1:N
        @constraint(model, y[i]==yp[i]+yn[i])
        @constraint(model, M*??_long[i]>=yp[i])
        @constraint(model, ??_long[i]<=M*yp[i])
        @constraint(model, M*??_short[i]>=yn[i])
        @constraint(model, ??_short[i]<=M*yn[i])
    end

    if only_long
        
        for i=1:N
            @constraint(model, yn[i]==0.0)
        end
    else
        @constraint(model, sum(yp[i] for i=1:N)==nl)
        @constraint(model, sum(yn[i] for i=1:N)==ns)
    end

    K=size(S)[2]

    for k in 1:K
        @constraint(model, sum((yp[i]+yn[i])*S[i,k] for i=1:N)<=sum((yp[i]+yn[i]) for i=1:N)*se)
    end

    @constraint(model, sum(y[i] for i=1:N)==50.0)

    #@objective(model, Max, sum((??[i]/??[i])*??[i]/100 for i=1:N));
    @objective(model, Max, sum((??[i])*??[i]/100 for i=1:N));
  
    optimize!(model)
 
    ??_value = value.(??)
    y_value = value.(y)
    yp_value = value.(yp)
    yn_value = value.(yn)
   
    return ??_value, y_value, yp_value, yn_value
end


function optimizationRevolut(??, GRB_ENV=Gurobi.Env())

    model=JuMP.Model(with_optimizer(() -> Gurobi.Optimizer(GRB_ENV)))
    set_silent(model) 
    # Number of assets
    N=length(??)
    @variable(model, y[1:N], Bin) #Easy hold one stock strategy
    @objective(model, Max, sum(??[i]*y[i] for i=1:N));
    # Only one stock
    @constraint(model, sum(y[i] for i=1:N)==1.0)

    optimize!(model)
    return value.(y)
end

function rebalance_optimization(??, ??, ??n, u_up, u_down, L, S; min_position=0.03, nlong=25, nshort=25,
                                 use_sector_exposure=false, sector_exposure=0.15, ?????=0.0, GRB_ENV=[], ??nasdaq=0.0, free_longshort=false)

    #Creates the JuMP model with the Solver 
    #model=Model(with_optimizer(CPLEX.Optimizer))
    model=JuMP.Model(with_optimizer(() -> Gurobi.Optimizer(GRB_ENV)))
    # No verbose
    set_silent(model) 
    # Number of assets
    N=length(??)

    @variable(model, u_down[i] <= ??[i=1:N] <= u_up[i]) # u_down ??? ?? ??? u_up
    @variable(model, ??_c ) # Cash

    #For leverage we need the long and short positions
    @variable(model, 0.0 <= ??_long[1:N])
    @variable(model, 0.0 <= ??_short[1:N])
    #Redefinition ??=??+ - ??-
    for i=1:N
        @constraint(model, ??[i]==??_long[i]-??_short[i])
    end
    # Budget constraint
    # Turns out quantopiuan doesnt have this constraint
    #
    @constraint(model, sum(??[i] for i=1:N)+ ??_c== 1.0)
    # ! Only for SquarePoint testing
    # Hold 50 companies # Hold 25 long and 25 short

    # Create some binary variables to see if we have a company either long or short
    @variable(model, yp[1:N], Bin)
    @variable(model, yn[1:N], Bin)
    @variable(model, y[1:N], Bin)

    # So, if ?? is different than zero the binary variable activates

    # M*?? >= y 
    # ?? <= M*y
    # If y=1 ??>0 , if y=0 ?? has to be zero
    # If ??>0 y has to be 1, if ??=0 y has to be zerp
    M=20000000.0
    for i in 1:N
        @constraint(model, y[i]==yp[i]+yn[i])
        @constraint(model, M*??_long[i]>=yp[i])
        @constraint(model, ??_long[i]<=M*yp[i])
        @constraint(model, M*??_short[i]>=yn[i])
        @constraint(model, ??_short[i]<=M*yn[i])
        @constraint(model, yp[i]*min_position<=??_long[i])
        @constraint(model, yn[i]*min_position<=??_short[i])
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
    #@constraint(model, -0.05 <= sum(??[i]*??[i] for i=1:N) <= 0.05)
    if ?????!=-99
        @constraint(model,  sum(??[i]*??[i] for i=1:N) == ?????)
    end

    if ??nasdaq!=-99
        @constraint(model,  sum(??n[i]*??[i] for i=1:N) <= ??nasdaq+0.5)
        @constraint(model,  sum(??n[i]*??[i] for i=1:N) >= ??nasdaq-0.5)
    end

    # For leverage we use the split definition and create a linear constraint

    #@constraint(model, sum(??_long[i] for i=1:N) + sum(??_short[i] for i=1:N)<=1.0)
    #
    @constraint(model,sum(??_long[i] for i=1:N) +sum(??_short[i] for i=1:N)<=L*(sum(??_long[i] for i=1:N) - sum(??_short[i] for i=1:N)+??_c))

    #@constraint(model, (z_??^2)*sum(??[i]*??[i,j]*??[j] for i=1:N, j=1:N)<=(MADD-cD)^2);
    # Objective
    @objective(model, Max, sum(??[i]*??[i] for i=1:N));
  
    optimize!(model)
 
    ??_value = value.(??)
    ??c_value = value.(??_c)
    ??_long_value=value.(??_long)
    ??_short_value=value.(??_short)
    ??_value=??_value/sum(abs.(??_value))
    ??_long_value=??_long_value/sum(abs.(??_value))
    ??_short_value=??_short_value/sum(abs.(??_value))

    y_value=value.(y)

    return ??_value, ??c_value, ??_long_value, ??_short_value, y_value

 end