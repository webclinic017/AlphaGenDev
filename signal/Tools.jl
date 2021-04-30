#--------------------------------------------------------
# PROGRAM NAME - Tools 
#
# PROGRAMMER - J.F. Imbet, juan.imbet@upf.edu
#
# VERSION - 0.0.1    [Mayor].[Minor].[Patch]
#
# PROJECT - AlphaGen Investment Strategies
#
# USAGE - Call these useful tools in the backtest development
#
# REQUIRES - None
#
# SYSTEM - All
#
# DATE - Mar 27 2021  (Sat) 18:00
#
# BUGS - Not known
#	
#
# DESCRIPTION - None
#			
#			
#--------------------p=E[mx]------------------------------


"""
`quantopian_symbols(year::Int64)`

# Arguments
`year::Int64` Year to look for the symbols available in quantopian

# Returns
Vector of symbols
"""
function quantopian_symbols(y::Int64)
    file_symbols=CSV.read(string(y)*".txt", header=false)
    trym_symbol(x) = Symbol(x[2:length(x)-2])
    out=trym_symbol.(file_symbols.Column2)
    return out
end

nanmean(x)=mean(filter(!isnan, x))
nonan(x)=filter(!isnan, x)
#Date Conversion
num_date(x)= floor(Int64, parse(Float64,SubString(x, 1,4))*100+parse(Float64,SubString(x, 6,length(x))))

tmonth(x)=Dates.yearmonth(x)[1]*100+Dates.yearmonth(x)[2]

num_date2(x) = floor(Int64, parse(Float64,SubString(x, 1,4))*100+parse(Float64,SubString(x, 6,7)))

function reduce_df(df, symbols, signal_symbols, betas_symbols)
    N=length(symbols)
    col_sums=[sum(df[:,i]) for i=1:N]
    ret_symbols=[Symbol(symbols[i]) for i=1:N if !ismissing(col_sums[i])]
    ret_symbols=ret_symbols[(ret_symbols .∈ [signal_symbols]).* (ret_symbols .∈ [betas_symbols])]
    df[:, ret_symbols], ret_symbols
end


function month_string(x)
    m=(round(Int64, x-round(Int64, floor(x/100))*100))
    if m<10
        return "0"*string(m)
    else
        return string(m)
    end
end

function year_string(x)
    y=(round(Int64, round(Int64, floor(x/100)-2000)))
    if y<10
        return "0"*string(y)
    else
        return string(y)
    end
end

date_csv_format(x)= month_string(x)*"/15/"*year_string(x)

qdate(x) = string(round(Int64, floor(x/100)), "-",month_string(x) , "-28")

function string_to_date(x)

    day=parse(Int64,x[1:2])
    month=x[3:5]
    m=0
    if month=="jan"
        m=1
    elseif month=="feb"
        m=2
    elseif month=="mar"
        m=3
    elseif month=="apr"
        m=4
    elseif month=="may"
        m=5
    elseif month=="jun"
        m=6
    elseif month=="jul"
        m=7
    elseif month=="aug"
        m=8
    elseif month=="sep"
        m=9
    elseif month=="oct"
        m=10
    elseif month=="nov"
        m=11
    elseif month=="dec"
        m=12
    end

    year=parse(Int64, x[6:9])
    return Date(year, m, day)
end

"""
    Converts a categorical vector to a dummy matrix

    e.g. [1 2 1] -> [1 0; 0 1; 1 0]

"""
function cate_to_mat(x)
    # Unique values
    unique_x=unique(x)
    x[ismissing.(x)] .=-1
    unique_x[ismissing.(unique_x)] .=-1
    K=size(unique_x)[1]
    N=size(x)[1]
    S=zeros(N,K)
    for n in 1:N
        for k in 1:K
            S[n,k]=x[n]==unique_x[k]   
        end
    end
    
    return S
end

"""
    `yyyymmddToDate(date)`

    Converts a date in Int format yyyymmdd to object Date
    e.g. 20120101 -> Dates.Date(2012,1,1)
"""
function yyyymmddToDate(date)
    yyyy= convert(Int64, floor(date/10000))
    mm  = convert(Int64, floor(date/100)-yyyy*100)
    dd  = convert(Int64, date- floor(date/100)*100) 
    return Dates.Date(yyyy, mm, dd)
end

"""
    `newcol(df,col)`
Creates a new column with symbol col in dataframe df with missing values that can take
the value of floats

e.g. @newcol df :returns

"""
macro newcol(df, col)
    return :( $df[!, $col]=missings(Float64, size($df)[1]) )
end

#Statistical functions that take into account missings

"""
x=[1, 5, 6, 7, missing]
mean(x)
fmissing(mean, x)
"""
fmissing(f, x)= f(filter(t-> !any(g -> g(t), (ismissing, isnothing, isnan)), x))

function computeReturnColumn!(df; adjCloseColumn="Adj Close")
  #@newcol df :ret
  df[!, :ret]=missings(Float64, size(df)[1])
  for i in 2:size(df)[1]
    df.ret[i]= df[i, [adjCloseColumn]][1]/df[i-1, [adjCloseColumn]][1] - 1.0
  end
 
end

function twoDigitSIC()
    names=Dict()
    names[1] = "Agricultural Production – Crops"
    names[2] = "Agricultural Production – Livestock"
    names[7] = "Agricultural Services"
    names[8] = "Forestry"
    names[9] = "Fishing, Hunting, & Trapping"
    names[10]= "Metal, Mining"
    names[12]= "Coal Mining"
    names[13]= "Oil & Gas Extraction"
    names[14]= "Nonmetallic Minerals, Except Fuels"
    names[15]= "General Building Contractors"
    names[16]= "Heavy Construction, Except Building"
    names[17]= "Special Trade Contractors"
    names[20]= "Food & Kindred Products"
    names[21]= "Tobacco Products"
    names[22]= "Textile Mill Products"
    names[23]= "Apparel & Other Textile Products"
    names[24]= "Lumber & Wood Products"
    names[25]= "Furniture & Fixtures"
    names[26]= "Paper & Allied Products"
    names[27]= "Printing & Publishing"
    names[28]= "Chemical & Allied Products"
    names[29]= "Petroleum & Coal Products"
    names[30]= "Rubber & Miscellaneous Plastics Products"
    names[31]= "Leather & Leather Products"
    names[32]= "Stone, Clay, & Glass Products"
    names[33]= "Primary Metal Industries"
    names[34]= "Fabricated Metal Products"
    names[35]= "Industrial Machinery & Equipment"
    names[36]= "Electronic & Other Electric Equipment"
    names[37]= "Transportation Equipment"
    names[38]= "Instruments & Related Products"
    names[39]= "Miscellaneous Manufacturing Industries"
    names[40]= "Railroad Transportation"
    names[41]= "Local & Interurban Passenger Transit"
    names[42]= "Trucking & Warehousing"
    names[43]= "U.S. Postal Service"
    names[44]= "Water Transportation"
    names[45]= "Transportation by Air"
    names[46]= "Pipelines, Except Natural Gas"
    names[47]= "Transportation Services"
    names[48]= "Communications"
    names[49]= "Electric, Gas, & Sanitary Services"
    names[50]= "Wholesale Trade – Durable Goods"
    names[51]= "Wholesale Trade – Nondurable Goods"
    names[52]= "Building Materials & Gardening Supplies"
    names[53]= "General Merchandise Stores"
    names[54]= "Food Stores"
    names[55]= "Automative Dealers & Service Stations"
    names[56]= "Apparel & Accessory Stores"
    names[57]= "Furniture & Homefurnishings Stores"
    names[58]= "Eating & Drinking Places"
    names[59]= "Miscellaneous Retail"
    names[60]= "Depository Institutions"
    names[61]= "Nondepository Institutions"
    names[62]= "Security & Commodity Brokers"
    names[63]= "Insurance Carriers"
    names[64]= "Insurance Agents, Brokers, & Service"
    names[65]= "Real Estate"
    names[67]= "Holding & Other Investment Offices"
    names[70]= "Hotels & Other Lodging Places"
    names[72]= "Personal Services"
    names[73]= "Business Services"
    names[75]= "Auto Repair, Services, & Parking"
    names[76]= "Miscellaneous Repair Services"
    names[78]= "Motion Pictures"
    names[79]= "Amusement & Recreation Services"
    names[80]= "Health Services"
    names[81]= "Legal Services"
    names[82]= "Educational Services"
    names[83]= "Social Services"
    names[84]= "Museums, Botanical, Zoological Gardens"
    names[86]= "Membership Organizations"
    names[87]= "Engineering & Management Services"
    names[88]= "Private Households"
    names[89]= "Services, Not Elsewhere Classified"
    names[91]= "Executive, Legislative, & General"
    names[92]= "Justice, Public Order, & Safety"
    names[93]= "Finance, Taxation, & Monetary Policy"
    names[94]= "Administration of Human Resources"
    names[95]= "Environmental Quality & Housing"
    names[96]= "Administration of Economic Programs"
    names[97]= "National Security & International Affairs"
    names[98]= "Zoological Gardens"
    names[99]= "Non-Classifiable Establishments"

    return names
end



function sicCodes(industry)
    #Possible sicCodes: NoDur,Durbl,Manuf,Enrgy,HiTec,Telcm,Shops,Hlth ,Utils,Other
    sics=[]
    if industry==:NoDur #  Consumer Nondurables -- Food, Tobacco, Textiles, Apparel, Leather, Toys
        append!(sics, collect(0100:0999))
        append!(sics, collect(2000:2399))
        append!(sics, collect(2700:2749))
        append!(sics, collect(2770:2799))
        append!(sics, collect(3100:3199))
        append!(sics, collect(3940:3989))
    elseif industry==:Durbl  #Consumer Durables -- Cars, TVs, Furniture, Household Appliances
        append!(sics, collect(2500:2519))
        append!(sics, collect(2590:2599))
        append!(sics, collect(3630:3659))
        append!(sics, collect(3710:3711))
        append!(sics, collect(3714:3714))
        append!(sics, collect(3716:3716))
        append!(sics, collect(3750:3751))
        append!(sics, collect(3792:3792))
        append!(sics, collect(3900:3939))
        append!(sics, collect(3990:3999))
    elseif industry==:Manuf  #Manufacturing -- Machinery, Trucks, Planes, Chemicals, Off Furn, Paper, Com Printing
        append!(sics, collect(2520:2589))
        append!(sics, collect(2600:2699))
        append!(sics, collect(2750:2769))
        append!(sics, collect(2800:2829))
        append!(sics, collect(2840:2899))
        append!(sics, collect(3000:3099))
        append!(sics, collect(3200:3569))
        append!(sics, collect(3580:3621))
        append!(sics, collect(3623:3629))
        append!(sics, collect(3700:3709))
        append!(sics, collect(3712:3713))
        append!(sics, collect(3715:3715))
        append!(sics, collect(3717:3749))
        append!(sics, collect(3752:3791))
        append!(sics, collect(3793:3799))
        append!(sics, collect(3860:3899))
    elseif industry==:Enrgy  #Oil, Gas, and Coal Extraction and Products
        append!(sics, collect(1200:1399))
        append!(sics, collect(2900:2999))
    elseif industry==:HiTec  #Business Equipment -- Computers, Software, and Electronic Equipment
        append!(sics, collect(3570:3579)) #
        append!(sics, collect(3622:3622)) # Industrial controls
        append!(sics, collect(3660:3692)) #
        append!(sics, collect(3694:3699)) #
        append!(sics, collect(3810:3839)) #
        append!(sics, collect(7370:7372)) # Services - computer programming and data processing
        append!(sics, collect(7373:7373)) # Computer integrated systems design
        append!(sics, collect(7374:7374)) # Services - computer processing, data preparation and processing  
        append!(sics, collect(7375:7375)) # Services - information retrieval services
        append!(sics, collect(7376:7376)) # Services - computer facilities management service
        append!(sics, collect(7377:7377)) # Services - computer rental and leasing
        append!(sics, collect(7378:7378)) # Services - computer maintenance and repair
        append!(sics, collect(7379:7379)) # Services - computer related services
        append!(sics, collect(7391:7391)) # Services - R&D labs
        append!(sics, collect(8730:8734)) # Services - research, development, testing labs
    elseif industry==:Telcm  #Telephone and Television Transmission
        append!(sics, collect(4800:4899))
    elseif industry==:Shops  #Wholesale, Retail, and Some Services (Laundries, Repair Shops)
        append!(sics, collect(5000:5999))
        append!(sics, collect(7200:7299))
        append!(sics, collect(7600:7699))
    elseif industry==:Hlth   #Healthcare, Medical Equipment, and Drugs
        append!(sics, collect(2830:2839))
        append!(sics, collect(3693:3693))
        append!(sics, collect(3840:3859))
        append!(sics, collect(8000:8099))
    elseif industry==:Utils  #Utilities
        append!(sics, collect(4900:4949))
    else  #Other -- Mines, Constr, BldMt, Trans, Hotels, Bus Serv, Entertainment, Finance
        sics=[]
    end
    return sics
end



function graphMonthlyReturns(time_span, ret_portfolio)
    #Alpha month by month
    function t_month(x)
        Dates.format(x, "u-yy")
    end
    months=unique(t_month.(time_span))
    
    avg_r=[]
    for mm in months
        t= t_month.(time_span) .== mm
        temp=ret_portfolio[t]
        temp[temp .> percentile(temp,99)] .= 0.0
        ret=prod(1+temp[i] for i=1:size(temp)[1])-1
        push!(avg_r, ret*100)
    end

    plot()
    bar!(months, avg_r, label = "Relative Monthly Return", 
         legend=:topleft, xrotation=45)
    title!("Realized monthly return")
    ylabel!("Realized return %")

    println("Average Relative Monthly Return $(mean(avg_r)) %")
    println("S.D. Relative Monthly Return $(std(avg_r)) %")
    #savefig()
end


function quarterAnalysis(time_span, ret_portfolio, ret_benchmark)

    



end