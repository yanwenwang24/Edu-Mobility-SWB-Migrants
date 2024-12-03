## ------------------------------------------------------------------------
##
## Script name: 04.1_tidy_ESS4_AT.jl
## Purpose: Clean ESS4_AT data
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

ESS4_AT = ESS["ESS4_AT"]

# 2 Select and construct variables ----------------------------------------

# 2.1 Relationships --------------------------------------------------------

# Create unique ID
@transform!(ESS4_AT, :pid = string.(Int.(:idno), :cntry))

# Select variables (relations, genders, year born)
ESS4_AT_rship = select(ESS4_AT, :pid, names(ESS4_AT, r"^rship"))
ESS4_AT_gndr = select(ESS4_AT, :pid, names(ESS4_AT, r"^gndr"))
select!(ESS4_AT_gndr, Not(:gndr))
ESS4_AT_yrbrn = select(ESS4_AT, :pid, names(ESS4_AT, r"^yrbrn"))
select!(ESS4_AT_yrbrn, Not(:yrbrn))

# Transform into long format
ESS4_AT_rship_long = stack(
    ESS4_AT_rship,
    Not(:pid),
    variable_name="rank",
    value_name="rship"
)

ESS4_AT_gndr_long = stack(
    ESS4_AT_gndr,
    Not(:pid),
    variable_name="rank",
    value_name="gndr"
)

ESS4_AT_yrbrn_long = stack(
    ESS4_AT_yrbrn,
    Not(:pid),
    variable_name="rank",
    value_name="yrbrn"
)

# Clean up rank column to keep only numbers
transform!(
    ESS4_AT_rship_long,
    :rank => ByRow(x -> replace(string(x), "rshipa" => "")) => :rank
)

transform!(
    ESS4_AT_gndr_long,
    :rank => ByRow(x -> replace(string(x), "gndr" => "")) => :rank
)

transform!(
    ESS4_AT_yrbrn_long,
    :rank => ByRow(x -> replace(string(x), "yrbrn" => "")) => :rank
)

ESS4_AT_relations_long = leftjoin(ESS4_AT_rship_long, ESS4_AT_gndr_long, on=[:pid, :rank])
ESS4_AT_relations_long = leftjoin(ESS4_AT_relations_long, ESS4_AT_yrbrn_long, on=[:pid, :rank])

# Get respondent's own gender and year born
ESS4_AT_relations_long = leftjoin(
    ESS4_AT_relations_long,
    select(ESS4_AT, :pid, :gndr, :yrbrn),
    on=:pid,
    makeunique=true
)

# 2.2 Children ------------------------------------------------------------

ESS4_AT_child = @chain ESS4_AT_relations_long begin
    @subset(:rship .== 2)
    @groupby(:pid)
    @combine(:child_count = length(:pid))
    @transform(:child_count = ifelse.(:child_count .>= 3, 3, :child_count))
    @transform(:child_present = 1)
end

ESS4_AT_child_under6 = @chain ESS4_AT_relations_long begin
    @subset(:rship .== 2, :yrbrn .>= 2008 - 6)
    @groupby(:pid)
    @combine(:child_under6_count = length(:pid))
    @transform(:child_under6_count = ifelse.(:child_under6_count .>= 3, 3, :child_under6_count))
    @transform(:child_under6_present = 1)
end

leftjoin!(ESS4_AT, ESS4_AT_child, on=:pid)
leftjoin!(ESS4_AT, ESS4_AT_child_under6, on=:pid)

ESS4_AT = @chain ESS4_AT begin
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
    ESS4_AT,
    :pid,
    :essround,
    :cntry,
    :pspwght, :pweight,
    :stflife => :lsat,
    :gndr => :female,
    :agea => :age,
    :maritala => :marital,
    :hhmmb => :hhsize,
    :dvrcdev,
    :ctzcntr,
    :brncntr,
    :cntbrthb => :cntbrth,
    :livecntr,
    :blgetmg => :minority,
    :facntr,
    :fbrncnta => :fbrncnt,
    :mocntr,
    :mbrncnta => :mbrncnt,
    :edulvla => :edu_r,
    :edulvlfa => :edu_f,
    :edulvlma => :edu_m,
    :uempla, :uempli,
    :hincfel,
    :child_count, :child_present,
    :child_under6_count, :child_under6_present
)

ESS4_AT = @chain ESS4_AT begin
    @transform(:year = 2008)
    @transform(:female = recode(:female, 1 => 0, 2 => 1, missing => missing))
    @transform(:anweight = :pspwght .* :pweight)
    @transform(
        :mstat = recode(
            :marital,
            1 => "married",
            2 => "married",
            3 => "unpartnered",
            4 => "unpartnered",
            5 => "unpartnered",
            6 => "unpartnered",
            7 => "unpartnered",
            8 => "unpartnered",
            9 => "never-married",
            missing => missing
        )
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
            4 => 3,
            5 => 4,
            55 => missing,
            missing => missing
        ),
        :edu4_f = recode(
            :edu_f,
            0 => missing,
            1 => 1,
            2 => 1,
            3 => 2,
            4 => 3,
            5 => 4,
            55 => missing,
            missing => missing
        ),
        :edu4_m = recode(
            :edu_m,
            0 => missing,
            1 => 1,
            2 => 1,
            3 => 2,
            4 => 3,
            5 => 4,
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

# Ever-divorced
divorce = Vector{Union{Int,Missing}}(undef, nrow(ESS4_AT))

for i in 1:nrow(ESS4_AT)
    local marital = coalesce(ESS4_AT.:marital[i], 99)
    local dvrcdev = coalesce(ESS4_AT.:dvrcdev[i], 99)

    if marital == 5 || dvrcdev == 1
        divorce[i] = 1
    elseif marital == 9 || dvrcdev == 2
        divorce[i] = 0
    else
        divorce[i] = missing
    end
end

ESS4_AT[!, :divorce] = divorce

# 2.4 Select variables ----------------------------------------------------

# Select and rename variables
select!(
    ESS4_AT,
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

Arrow.write("Datasets_tidy/ESS4_AT.arrow", ESS4_AT)