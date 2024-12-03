## ------------------------------------------------------------------------
##
## Script name: 02_tidy_ESS2.jl
## Purpose: Clean ESS2 data
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

ESS2 = ESS["ESS2"]

# 2 Select and construct variables ----------------------------------------

# 2.1 Relationships --------------------------------------------------------

# Create unique ID
@transform!(ESS2, :pid = string.(Int.(:idno), :cntry))

# Select variables (relations, genders, year born)
ESS2_rship = select(ESS2, :pid, names(ESS2, r"^rship"))
ESS2_gndr = select(ESS2, :pid, names(ESS2, r"^gndr"))
select!(ESS2_gndr, Not(:gndr))
ESS2_yrbrn = select(ESS2, :pid, names(ESS2, r"^yrbrn"))
select!(ESS2_yrbrn, Not(:yrbrn))

# Transform into long format
ESS2_rship_long = stack(
    ESS2_rship,
    Not(:pid),
    variable_name="rank",
    value_name="rship"
)

ESS2_gndr_long = stack(
    ESS2_gndr,
    Not(:pid),
    variable_name="rank",
    value_name="gndr"
)

ESS2_yrbrn_long = stack(
    ESS2_yrbrn,
    Not(:pid),
    variable_name="rank",
    value_name="yrbrn"
)

# Clean up rank column to keep only numbers
transform!(
    ESS2_rship_long,
    :rank => ByRow(x -> replace(string(x), "rshipa" => "")) => :rank
)

transform!(
    ESS2_gndr_long,
    :rank => ByRow(x -> replace(string(x), "gndr" => "")) => :rank
)

transform!(
    ESS2_yrbrn_long,
    :rank => ByRow(x -> replace(string(x), "yrbrn" => "")) => :rank
)

ESS2_relations_long = leftjoin(ESS2_rship_long, ESS2_gndr_long, on=[:pid, :rank])
ESS2_relations_long = leftjoin(ESS2_relations_long, ESS2_yrbrn_long, on=[:pid, :rank])

# Get respondent's own gender and year born
ESS2_relations_long = leftjoin(
    ESS2_relations_long,
    select(ESS2, :pid, :gndr, :yrbrn),
    on=:pid,
    makeunique=true
)

# 2.2 Children ------------------------------------------------------------

ESS2_child = @chain ESS2_relations_long begin
    @subset(:rship .== 2)
    @groupby(:pid)
    @combine(:child_count = length(:pid))
    @transform(:child_count = ifelse.(:child_count .>= 3, 3, :child_count))
    @transform(:child_present = 1)
end

ESS2_child_under6 = @chain ESS2_relations_long begin
    @subset(:rship .== 2, :yrbrn .>= 2004 - 6)
    @groupby(:pid)
    @combine(:child_under6_count = length(:pid))
    @transform(:child_under6_count = ifelse.(:child_under6_count .>= 3, 3, :child_under6_count))
    @transform(:child_under6_present = 1)
end

leftjoin!(ESS2, ESS2_child, on=:pid)
leftjoin!(ESS2, ESS2_child_under6, on=:pid)

ESS2 = @chain ESS2 begin
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
    ESS2,
    :pid,
    :essround,
    :cntry,
    :pspwght, :pweight,
    :stflife => :lsat,
    :gndr => :female,
    :agea => :age,
    :marital,
    :hhmmb => :hhsize,
    :dvrcdev,
    :ctzcntr,
    :ctzshipa => :ctzship,
    :brncntr,
    :cntbrtha => :cntbrth,
    :livecntr,
    :blgetmg => :minority,
    :facntr,
    :fbrncnt,
    :mocntr,
    :mbrncnt,
    :edulvla => :edu_r,
    :edulvlfa => :edu_f,
    :edulvlma => :edu_m,
    :uempla, :uempli,
    :hincfel,
    :child_count, :child_present,
    :child_under6_count, :child_under6_present
)

ESS2 = @chain ESS2 begin
    @transform(:year = 2004)
    @transform(:female = recode(:female, 1 => 0, 2 => 1, missing => missing))
    @transform(:anweight = :pspwght .* :pweight)
    @transform(
        :mstat = recode(
            :marital,
            1 => "married",
            2 => "unpartnered",
            3 => "unpartnered",
            4 => "unpartnered",
            5 => "never-married",
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
divorce = Vector{Union{Int,Missing}}(undef, nrow(ESS2))

for i in 1:nrow(ESS2)
    local marital = coalesce(ESS2.:marital[i], 99)
    local dvrcdev = coalesce(ESS2.:dvrcdev[i], 99)

    if marital == 3 || dvrcdev == 1
        divorce[i] = 1
    elseif marital == 5 || dvrcdev == 2
        divorce[i] = 0
    else
        divorce[i] = missing
    end
end

ESS2[!, :divorce] = divorce

# 2.4 Select variables ----------------------------------------------------

# Select and rename variables
select!(
    ESS2,
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

Arrow.write("Datasets_tidy/ESS2.arrow", ESS2)