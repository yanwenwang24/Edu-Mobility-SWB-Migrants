## ------------------------------------------------------------------------
##
## Script name: 07_tidy_ESS7.jl
## Purpose: Clean ESS7 data
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

ESS7 = ESS["ESS7"]

# 2 Select and construct variables ----------------------------------------

# 2.1 Relationships --------------------------------------------------------

# Create unique ID
@transform!(ESS7, :pid = string.(Int.(:idno), :cntry))

# Select variables (relations, genders, year born)
ESS7_rship = select(ESS7, :pid, names(ESS7, r"^rship"))
ESS7_gndr = select(ESS7, :pid, names(ESS7, r"^gndr"))
select!(ESS7_gndr, Not(:gndr))
ESS7_yrbrn = select(ESS7, :pid, names(ESS7, r"^yrbrn"))
select!(ESS7_yrbrn, Not(:yrbrn))

# Transform into long format
ESS7_rship_long = stack(
    ESS7_rship,
    Not(:pid),
    variable_name="rank",
    value_name="rship"
)

ESS7_gndr_long = stack(
    ESS7_gndr,
    Not(:pid),
    variable_name="rank",
    value_name="gndr"
)

ESS7_yrbrn_long = stack(
    ESS7_yrbrn,
    Not(:pid),
    variable_name="rank",
    value_name="yrbrn"
)

# Clean up rank column to keep only numbers
transform!(
    ESS7_rship_long,
    :rank => ByRow(x -> replace(string(x), "rshipa" => "")) => :rank
)

transform!(
    ESS7_gndr_long,
    :rank => ByRow(x -> replace(string(x), "gndr" => "")) => :rank
)

transform!(
    ESS7_yrbrn_long,
    :rank => ByRow(x -> replace(string(x), "yrbrn" => "")) => :rank
)

ESS7_relations_long = leftjoin(ESS7_rship_long, ESS7_gndr_long, on=[:pid, :rank])
ESS7_relations_long = leftjoin(ESS7_relations_long, ESS7_yrbrn_long, on=[:pid, :rank])

# Get respondent's own gender and year born
ESS7_relations_long = leftjoin(
    ESS7_relations_long,
    select(ESS7, :pid, :gndr, :yrbrn),
    on=:pid,
    makeunique=true
)

# 2.2 Children ------------------------------------------------------------

ESS7_child = @chain ESS7_relations_long begin
    @subset(:rship .== 2)
    @groupby(:pid)
    @combine(:child_count = length(:pid))
    @transform(:child_count = ifelse.(:child_count .>= 3, 3, :child_count))
    @transform(:child_present = 1)
end

ESS7_child_under6 = @chain ESS7_relations_long begin
    @subset(:rship .== 2, :yrbrn .>= 2014 - 6)
    @groupby(:pid)
    @combine(:child_under6_count = length(:pid))
    @transform(:child_under6_count = ifelse.(:child_under6_count .>= 3, 3, :child_under6_count))
    @transform(:child_under6_present = 1)
end

leftjoin!(ESS7, ESS7_child, on=:pid)
leftjoin!(ESS7, ESS7_child_under6, on=:pid)

ESS7 = @chain ESS7 begin
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
    ESS7,
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
    :ctzshipc => :ctzship,
    :brncntr,
    :cntbrthc => :cntbrth,
    :livecnta => :livecntr,
    :blgetmg => :minority,
    :facntr,
    :fbrncntb => :fbrncnt,
    :mocntr,
    :mbrncntb => :mbrncnt,
    :eisced => :edu_r,
    :eiscedf => :edu_f,
    :eiscedm => :edu_m,
    :uempla, :uempli,
    :hincfel,
    :child_count, :child_present,
    :child_under6_count, :child_under6_present
)

ESS7 = @chain ESS7 begin
    @transform(:year = 2014)
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
mstat = Vector{Union{String,Missing}}(undef, nrow(ESS7))

for i in 1:nrow(ESS7)
    local rshpsts = coalesce(ESS7.rshpsts[i], 99)
    local marsts = coalesce(ESS7.marsts[i], 99)

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

ESS7[!, :mstat] = mstat

# 2.4 Select variables ----------------------------------------------------

# Select and rename variables
select!(
    ESS7,
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
    :ctzship,
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

Arrow.write("Datasets_tidy/ESS7.arrow", ESS7)