## ------------------------------------------------------------------------
##
## Script name: 05_tidy_ESS5.jl
## Purpose: Clean ESS5 data
## Author: Yanwen Wang
## Date Created: 2024-12-03
## Email: yanwenwang@u.nus.edu
##
## ------------------------------------------------------------------------
##
## Notes:
##
## ------------------------------------------------------------------------

# 1 Load data -------------------------------------------------------------     

ESS5 = ESS["ESS5"]

# 2 Select and construct variables ----------------------------------------

# 2.1 Relationships --------------------------------------------------------

# Create unique ID
@transform!(ESS5, :pid = string.(Int.(:idno), :cntry))

# Select variables (relations, genders, year born)
ESS5_rship = select(ESS5, :pid, names(ESS5, r"^rship"))
ESS5_gndr = select(ESS5, :pid, names(ESS5, r"^gndr"))
select!(ESS5_gndr, Not(:gndr))
ESS5_yrbrn = select(ESS5, :pid, names(ESS5, r"^yrbrn"))
select!(ESS5_yrbrn, Not(:yrbrn))

# Transform into long format
ESS5_rship_long = stack(
    ESS5_rship,
    Not(:pid),
    variable_name="rank",
    value_name="rship"
)

ESS5_gndr_long = stack(
    ESS5_gndr,
    Not(:pid),
    variable_name="rank",
    value_name="gndr"
)

ESS5_yrbrn_long = stack(
    ESS5_yrbrn,
    Not(:pid),
    variable_name="rank",
    value_name="yrbrn"
)

# Clean up rank column to keep only numbers
transform!(
    ESS5_rship_long,
    :rank => ByRow(x -> replace(string(x), "rshipa" => "")) => :rank
)

transform!(
    ESS5_gndr_long,
    :rank => ByRow(x -> replace(string(x), "gndr" => "")) => :rank
)

transform!(
    ESS5_yrbrn_long,
    :rank => ByRow(x -> replace(string(x), "yrbrn" => "")) => :rank
)

ESS5_relations_long = leftjoin(ESS5_rship_long, ESS5_gndr_long, on=[:pid, :rank])
ESS5_relations_long = leftjoin(ESS5_relations_long, ESS5_yrbrn_long, on=[:pid, :rank])

# Get respondent's own gender and year born
ESS5_relations_long = leftjoin(
    ESS5_relations_long,
    select(ESS5, :pid, :gndr, :yrbrn),
    on=:pid,
    makeunique=true
)

# 2.2 Children ------------------------------------------------------------

ESS5_child = @chain ESS5_relations_long begin
    @subset(:rship .== 2)
    @groupby(:pid)
    @combine(:child_count = length(:pid))
    @transform(:child_count = ifelse.(:child_count .>= 3, 3, :child_count))
    @transform(:child_present = 1)
end

ESS5_child_under6 = @chain ESS5_relations_long begin
    @subset(:rship .== 2, :yrbrn .>= 2010 - 6)
    @groupby(:pid)
    @combine(:child_under6_count = length(:pid))
    @transform(:child_under6_count = ifelse.(:child_under6_count .>= 3, 3, :child_under6_count))
    @transform(:child_under6_present = 1)
end

leftjoin!(ESS5, ESS5_child, on=:pid)
leftjoin!(ESS5, ESS5_child_under6, on=:pid)

ESS5 = @chain ESS5 begin
    @transform(
        :child_count = coalesce.(:child_count, 0),
        :child_present = coalesce.(:child_present, 0),
        :child_under6_count = coalesce.(:child_under6_count, 0),
        :child_under6_present = coalesce.(:child_under6_present, 0)
    )
end

# 2.3 Other variables of interest -----------------------------------------

# Select and rename variables
select!(
    ESS5,
    :pid,
    :essround,
    :cntry,
    :pspwght, :pweight,
    :stflife => :lsat,
    :gndr => :female,
    :agea => :age,
    :rshpsts, :marsts,
    :hhmmb => :hhsize,
    :dvrcdeva => :divorce,
    :ctzcntr,
    :brncntr,
    :cntbrthb => :cntbrth,
    :livecnta => :livecntr,
    :blgetmg => :minority,
    :facntr,
    :fbrncnta => :fbrncnt,
    :mocntr,
    :mbrncnta => :mbrncnt,
    :eisced => :edu_r,
    :eiscedf => :edu_f,
    :eiscedm => :edu_m,
    :uempla, :uempli,
    :hincfel,
    :child_count, :child_present,
    :child_under6_count, :child_under6_present
)

ESS5 = @chain ESS5 begin
    @transform(:year = 2010)
    @transform(:female = recode(:female, 1 => 0, 2 => 1, missing => missing))
    @transform(:anweight = :pspwght .* :pweight)
    @transform(
        :divorce = recode(:divorce, 1 => 1, 2 => 0, missing => missing)
    )
    @transform(
        :minority = recode(:minority, 1 => 1, 2 => 0, missing => missing)
    )
    @transform(:hhsize =
        if ismissing(:hhsize)
            missing
        else
            min.(:hhsize, 6)
        end
    )
    @transform(
        :edu4_r = recode(
            :edu_r,
            0 => missing,
            1 => 1,
            2 => 1,
            3 => 2,
            4 => 2,
            5 => 3,
            6 => 4,
            7 => 4,
            55 => missing,
            missing => missing
        ),
        :edu4_f = recode(
            :edu_f,
            0 => missing,
            1 => 1,
            2 => 1,
            3 => 2,
            4 => 2,
            5 => 3,
            6 => 4,
            7 => 4,
            55 => missing,
            missing => missing
        ),
        :edu4_m = recode(
            :edu_m,
            0 => missing,
            1 => 1,
            2 => 1,
            3 => 2,
            4 => 2,
            5 => 3,
            6 => 4,
            7 => 4,
            55 => missing,
            missing => missing
        )
    )
    @transform(:uempl = ifelse.(:uempla .== 1 .|| :uempli .== 1, 1, 0))
    @transform(
        :hincfel = recode(
            :hincfel,
            1 => 1,
            2 => 0,
            3 => 0,
            4 => 0,
            missing => missing
        )
    )
end

# Marital status
mstat = Vector{Union{String,Missing}}(undef, nrow(ESS5))

for i in 1:nrow(ESS5)
    local rshpsts = coalesce(ESS5.rshpsts[i], 99)
    local marsts = coalesce(ESS5.marsts[i], 99)

    if rshpsts >= 1 && rshpsts <= 4 # Married
        mstat[i] = "married"
    elseif rshpsts >= 5 && rshpsts <= 6 # Unpartnered
        mstat[i] = "unpartnered"
    elseif marsts >= 1 && marsts <= 2 # Married
        mstat[i] = "married"
    elseif marsts >= 3 && marsts <= 5 # Unpartnered
        mstat[i] = "unpartnered"
    elseif marsts == 6 # Never-married
        mstat[i] = "never-married"
    else # Set all else to missing
        mstat[i] = missing
    end
end

ESS5[!, :mstat] = mstat

# 2.4 Select variables ----------------------------------------------------

# Select and rename variables
select!(
    ESS5,
    :pid,
    :year,
    :essround,
    :cntry,
    :anweight,
    :lsat,
    :female,
    :age,
    :mstat,
    :divorce,
    :ctzcntr,
    :brncntr,
    :cntbrth,
    :livecntr,
    :minority,
    :facntr,
    :fbrncnt,
    :mocntr,
    :mbrncnt,
    :hhsize,
    :edu4_r, :edu4_f, :edu4_m,
    :uempl,
    :hincfel,
    :child_count, :child_present,
    :child_under6_count, :child_under6_present
)

# 3 Save data -------------------------------------------------------------

Arrow.write("Datasets_tidy/ESS5.arrow", ESS5)