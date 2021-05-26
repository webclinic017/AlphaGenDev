df_sp500 = @from i in df_sp500 begin
    @where t-Day(252) <= i.t_day < t
    @select {i.adjcloseSPY, i.t_day,
            i.ret_adjcloseESF_5,
            i.ret_adjcloseESF_15, 
            i.ret_adjcloseESF_30,
            i.ret_adjcloseESF_45, 
            i.ret_adjcloseEURUSDX_5,
            i.ret_adjcloseEURUSDX_15, 
            i.ret_adjcloseEURUSDX_30, 
            i.ret_adjcloseEURUSDX_45, 
            i.ret_adjcloseEWH_5, 
            i.ret_adjcloseEWH_15, 
            i.ret_adjcloseEWH_30, 
            i.ret_adjcloseEWH_45, 
            i.ret_adjcloseGBPUSDX_5, 
            i.ret_adjcloseGBPUSDX_15, 
            i.ret_adjcloseGBPUSDX_30, 
            i.ret_adjcloseGBPUSDX_45, 
            i.ret_adjcloseGCF_5, 
            i.ret_adjcloseGCF_15, 
            i.ret_adjcloseGCF_30, 
            i.ret_adjcloseGCF_45, 
            i.ret_adjcloseJPYX_5, 
            i.ret_adjcloseJPYX_15, 
            i.ret_adjcloseJPYX_30, 
            i.ret_adjcloseJPYX_45, 
            i.ret_adjcloseLQD_5, 
            i.ret_adjcloseLQD_15, 
            i.ret_adjcloseLQD_30, 
            i.ret_adjcloseLQD_45, 
            i.ret_adjcloseNQF_5, 
            i.ret_adjcloseNQF_15, 
            i.ret_adjcloseNQF_30, 
            i.ret_adjcloseNQF_45, 
            i.ret_adjcloseSPY_5, 
            i.ret_adjcloseSPY_15, 
            i.ret_adjcloseSPY_30, 
            i.ret_adjcloseSPY_45, 
            i.ret_adjcloseTIP_5, 
            i.ret_adjcloseTIP_15, 
            i.ret_adjcloseTIP_30, 
            i.ret_adjcloseTIP_45, 
            i.ret_adjcloseTNX_5, 
            i.ret_adjcloseTNX_15, 
            i.ret_adjcloseTNX_30, 
            i.ret_adjcloseTNX_45, 
            i.ret_adjcloseVFSTX_5, 
            i.ret_adjcloseVFSTX_15, 
            i.ret_adjcloseVFSTX_30, 
            i.ret_adjcloseVFSTX_45, 
            i.ret_adjcloseVIX_5, 
            i.ret_adjcloseVIX_15, 
            i.ret_adjcloseVIX_30, 
            i.ret_adjcloseVIX_45, 
            i.ret_adjcloseYMF_5, 
            i.ret_adjcloseYMF_15, 
            i.ret_adjcloseYMF_30, 
            i.ret_adjcloseYMF_45, 
            i.ret_adjcloseZWF_5, 
            i.ret_adjcloseZWF_15, 
            i.ret_adjcloseZWF_30, 
            i.ret_adjcloseZWF_45}
    @collect DataFrame 
end

#! To avoid look ahead bias I construct forward looking variables in sample, the code is in another file for the sake of length
df_sp500[!, "Fexret"]= lead(df_sp500.adjcloseSPY, 15) ./ df_sp500.adjcloseSPY .- 1

ols = lm(@formula(Fexret ~ ret_adjcloseESF_5 +
                            ret_adjcloseESF_15 + 
                            ret_adjcloseESF_30 +
                            ret_adjcloseESF_45 +
                            ret_adjcloseEURUSDX_5 +
                            ret_adjcloseEURUSDX_15 + 
                            ret_adjcloseEURUSDX_30 +
                            ret_adjcloseEURUSDX_45 +
                            ret_adjcloseEWH_5 +
                            ret_adjcloseEWH_15 + 
                            ret_adjcloseEWH_30 +
                            ret_adjcloseEWH_45 +
                            ret_adjcloseGBPUSDX_5 + 
                            ret_adjcloseGBPUSDX_15 + 
                            ret_adjcloseGBPUSDX_30 +
                            ret_adjcloseGBPUSDX_45 +
                            ret_adjcloseGCF_5 +
                            ret_adjcloseGCF_15 +
                            ret_adjcloseGCF_30 + 
                            ret_adjcloseGCF_45 +
                            ret_adjcloseJPYX_5 +
                            ret_adjcloseJPYX_15 + 
                            ret_adjcloseJPYX_30 +
                            ret_adjcloseJPYX_45 +
                            ret_adjcloseLQD_5 +
                            ret_adjcloseLQD_15 + 
                            ret_adjcloseLQD_30 +
                            ret_adjcloseLQD_45 +
                            ret_adjcloseNQF_5 +
                            ret_adjcloseNQF_15 + 
                            ret_adjcloseNQF_30 +
                            ret_adjcloseNQF_45 +
                            ret_adjcloseSPY_5 +
                            ret_adjcloseSPY_15 + 
                            ret_adjcloseSPY_30 +
                            ret_adjcloseSPY_45 +
                            ret_adjcloseTIP_5 +
                            ret_adjcloseTIP_15 + 
                            ret_adjcloseTIP_30 +
                            ret_adjcloseTIP_45 +
                            ret_adjcloseTNX_5 +
                            ret_adjcloseTNX_15 + 
                            ret_adjcloseTNX_30 +
                            ret_adjcloseTNX_45 +
                            ret_adjcloseVFSTX_5 + 
                            ret_adjcloseVFSTX_15 + 
                            ret_adjcloseVFSTX_30 +
                            ret_adjcloseVFSTX_45 +
                            ret_adjcloseVIX_5 +
                            ret_adjcloseVIX_15 + 
                            ret_adjcloseVIX_30 +
                            ret_adjcloseVIX_45 +
                            ret_adjcloseYMF_5 +
                            ret_adjcloseYMF_15 + 
                            ret_adjcloseYMF_30 +
                            ret_adjcloseYMF_45 +
                            ret_adjcloseZWF_5 +
                            ret_adjcloseZWF_15 + 
                            ret_adjcloseZWF_30 +
                            ret_adjcloseZWF_45), df_sp500)