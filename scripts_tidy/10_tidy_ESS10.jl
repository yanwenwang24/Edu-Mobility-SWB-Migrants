## ------------------------------------------------------------------------
##
## Script name: 10_tidy_ESS10.jl
## Purpose: Clean ESS10 data
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

ESS10 = ESS["ESS10"]

# 2 Select and construct variables ----------------------------------------

# 2.1 Relationships --------------------------------------------------------

# Create unique ID
@transform!(ESS10, :pid = string.(Int.(:idno), :cntry))

# Select variables (relations, genders, year born)
ESS10_rship = select(ESS10, :pid, names(ESS10, r"^rship"))
ESS10_gndr = select(ESS10, :pid, names(ESS10, r"^gndr"))
select!(ESS10_gndr, Not(:gndr))
ESS10_yrbrn = select(ESS10, :pid, names(ESS10, r"^yrbrn"))
select!(ESS10_yrbrn, Not(:yrbrn))

# Transform into long format
ESS10_rship_long = stack(
    ESS10_rship,
    Not(:pid),
    variable_name="rank",
    value_name="rship"
)

ESS10_gndr_long = stack(
    ESS10_gndr,
    Not(:pid),
    variable_name="rank",
    value_name="gndr"
)

ESS10_yrbrn_long = stack(
    ESS10_yrbrn,
    Not(:pid),
    variable_name="rank",
    value_name="yrbrn"
)

# Clean up rank column to keep only numbers
transform!(
    ESS10_rship_long,
    :rank => ByRow(x -> replace(string(x), "rshipa" => "")) => :rank
)

transform!(
    ESS10_gndr_long,
    :rank => ByRow(x -> replace(string(x), "gndr" => "")) => :rank
)

transform!(
    ESS10_yrbrn_long,
    :rank => ByRow(x -> replace(string(x), "yrbrn" => "")) => :rank
)

ESS10_relations_long = leftjoin(ESS10_rship_long, ESS10_gndr_long, on=[:pid, :rank])
ESS10_relations_long = leftjoin(ESS10_relations_long, ESS10_yrbrn_long, on=[:pid, :rank])

# Get respondent's own gender and year born
ESS10_relations_long = leftjoin(
    ESS10_relations_long,
    select(ESS10, :pid, :gndr, :yrbrn),
    on=:pid,
    makeunique=true
)

# 2.2 Children ------------------------------------------------------------

ESS10_child = @chain ESS10_relations_long begin
    @subset(:rship .== 2)
    @groupby(:pid)
    @combine(:child_count = length(:pid))
    @transform(:child_count = ifelse.(:child_count .>= 3, 3, :child_count))
    @transform(:child_present = 1)
end

ESS10_child_under6 = @chain ESS10_relations_long begin
    @subset(:rship .== 2, :yrbrn .>= 2020 - 6)
    @groupby(:pid)
    @combine(:child_under6_count = length(:pid))
    @transform(:child_under6_count = ifelse.(:child_under6_count .>= 3, 3, :child_under6_count))
    @transform(:child_under6_present = 1)
end

leftjoin!(ESS10, ESS10_child, on=:pid)
leftjoin!(ESS10, ESS10_child_under6, on=:pid)

ESS10 = @chain ESS10 begin
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
    ESS10,
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
    :cntbrthd => :cntbrth,
    :livecnta => :livecntr,
    :feethngr => :minority,
    :facntr,
    :fbrncntc => :fbrncnt,
    :mocntr,
    :mbrncntc => :mbrncnt,
    :eisced => :edu_r,
    :eiscedf => :edu_f,
    :eiscedm => :edu_m,
    :uempla, :uempli,
    :hincfel,
    :child_count, :child_present,
    :child_under6_count, :child_under6_present
)

ESS10 = @chain ESS10 begin
    @transform(:year = 2020)
    @transform(:female = recode(:female, 1 => 0, 2 => 1, missing => missing))
    @transform(:anweight = :pspwght .* :pweight)
    @transform(
        :divorce = recode(:divorce, 1 => 1, 2 => 0, missing => missing)
    )
    @transform(
        :minority = recode(:minority, 1 => 0, 2 => 1, missing => missing)
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
mstat = Vector{Union{String,Missing}}(undef, nrow(ESS10))

for i in 1:nrow(ESS10)
    local rshpsts = coalesce(ESS10.rshpsts[i], 99)
    local marsts = coalesce(ESS10.marsts[i], 99)

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

ESS10[!, :mstat] = mstat

# 2.4 Select variables ----------------------------------------------------

# Select and rename variables
select!(
    ESS10,
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

Arrow.write("Datasets_tidy/ESS10.arrow", ESS10)