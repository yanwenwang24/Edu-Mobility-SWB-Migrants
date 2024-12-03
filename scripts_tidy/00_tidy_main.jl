## ------------------------------------------------------------------------
##
## Script name: 00_tidy_main.jl
## Purpose: Master file to tidy the raw data
## Author: Yanwen Wang
## Date Created: 2024-12-03
## Email: yanwenwang@u.nus.edu
##
## ------------------------------------------------------------------------
##
## Notes:
##
## ------------------------------------------------------------------------

# Load the required packages
using Arrow
using CategoricalArrays
using DataFrames, DataFramesMeta
using Random
using RData

# Load data, dictionaries and functions
ESS = load("Datasets_raw/ESS.RData")

# Source scripts
@time include("01_tidy_ESS1.jl")
@time include("02_tidy_ESS2.jl")
@time include("02.1_tidy_ESS2_IT.jl")
@time include("03_tidy_ESS3.jl")
@time include("03.1_tidy_ESS3_LV.jl")
@time include("03.2_tidy_ESS3_RO.jl")
@time include("04_tidy_ESS4.jl")
@time include("04.1_tidy_ESS4_AT.jl")
@time include("04.2_tidy_ESS4_LT.jl")
# @time include("05_tidy_ESS5.jl")
# @time include("05.1_tidy_ESS5_AT.jl")
# @time include("06_tidy_ESS6.jl")
# @time include("07_tidy_ESS7.jl")
# @time include("08_tidy_ESS8.jl")
# @time include("09_tidy_ESS9.jl")
# @time include("10_tidy_ESS10.jl")
# @time include("11_tidy_merge.jl")