## ------------------------------------------------------------------------
##
## Script name: 03.1_tidy_ESS3_LV.jl
## Purpose: Clean ESS3_LV data
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

ESS3_LV = ESS["ESS3_LV"]

# 2 Select and construct variables ----------------------------------------

# 2.1 Relationships --------------------------------------------------------

# Create unique ID
@transform!(ESS3_LV, :pid = string.(Int.(:idno), :cntry))

# Select variables (relations, genders, year born)
ESS3_LV_rship = select(ESS3_LV, :pid, names(ESS3_LV, r"^rship"))
ESS3_LV_gndr = select(ESS3_LV, :pid, names(ESS3_LV, r"^gndr"))
select!(ESS3_LV_gndr, Not(:gndr))
ESS3_LV_yrbrn = select(ESS3_LV, :pid, names(ESS3_LV, r"^yrbrn"))
select!(ESS3_LV_yrbrn, Not(:yrbrn))

# Transform into long format
ESS3_LV_rship_long = stack(
    ESS3_LV_rship,
    Not(:pid),
    variable_name="rank",
    value_name="rship"
)

ESS3_LV_gndr_long = stack(
    ESS3_LV_gndr,
    Not(:pid),
    variable_name="rank",
    value_name="gndr"
)

ESS3_LV_yrbrn_long = stack(
    ESS3_LV_yrbrn,
    Not(:pid),
    variable_name="rank",
    value_name="yrbrn"
)

# Clean up rank column to keep only numbers
transform!(
    ESS3_LV_rship_long,
    :rank => ByRow(x -> replace(string(x), "rshipa" => "")) => :rank
)

transform!(
    ESS3_LV_gndr_long,
    :rank => ByRow(x -> replace(string(x), "gndr" => "")) => :rank
)

transform!(
    ESS3_LV_yrbrn_long,
    :rank => ByRow(x -> replace(string(x), "yrbrn" => "")) => :rank
)

ESS3_LV_relations_long = leftjoin(ESS3_LV_rship_long, ESS3_LV_gndr_long, on=[:pid, :rank])
ESS3_LV_relations_long = leftjoin(ESS3_LV_relations_long, ESS3_LV_yrbrn_long, on=[:pid, :rank])

# Get respondent's own gender and year born
ESS3_LV_relations_long = leftjoin(
    ESS3_LV_relations_long,
    select(ESS3_LV, :pid, :gndr, :yrbrn),
    on=:pid,
    makeunique=true
)

# 2.2 Children ------------------------------------------------------------

ESS3_LV_child = @chain ESS3_LV_relations_long begin
    @subset(:rship .== 2)
    @groupby(:pid)
    @combine(:child_count = length(:pid))
    @transform(:child_count = ifelse.(:child_count .>= 3, 3, :child_count))
    @transform(:child_present = 1)
end

ESS3_LV_child_under6 = @chain ESS3_LV_relations_long begin
    @subset(:rship .== 2, :yrbrn .>= 2006 - 6)
    @groupby(:pid)
    @combine(:child_under6_count = length(:pid))
    @transform(:child_under6_count = ifelse.(:child_under6_count .>= 3, 3, :child_under6_count))
    @transform(:child_under6_present = 1)
end

leftjoin!(ESS3_LV, ESS3_LV_child, on=:pid)
leftjoin!(ESS3_LV, ESS3_LV_child_under6, on=:pid)

ESS3_LV = @chain ESS3_LV begin
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
    ESS3_LV,
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

ESS3_LV = @chain ESS3_LV begin
    @transform(:year = 2006)
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
divorce = Vector{Union{Int,Missing}}(undef, nrow(ESS3_LV))

for i in 1:nrow(ESS3_LV)
    local marital = coalesce(ESS3_LV.:marital[i], 99)
    local dvrcdev = coalesce(ESS3_LV.:dvrcdev[i], 99)

    if marital == 5 || dvrcdev == 1
        divorce[i] = 1
    elseif marital == 9 || dvrcdev == 2
        divorce[i] = 0
    else
        divorce[i] = missing
    end
end

ESS3_LV[!, :divorce] = divorce

# 2.4 Select variables ----------------------------------------------------

# Select and rename variables
select!(
    ESS3_LV,
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
    :livecntr => :livecntr_time,
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

Arrow.write("Datasets_tidy/ESS3_LV.arrow", ESS3_LV)