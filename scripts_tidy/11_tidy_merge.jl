## ------------------------------------------------------------------------
##
## Script name: 11_tidy_merge.jl
## Purpose: Merge all cleaned ESS datasets
## Author: Yanwen Wang
## Date Created: 2024-12-03
## Email: yanwenwang@u.nus.edu
##
## ------------------------------------------------------------------------
##
## Notes:
##
## ------------------------------------------------------------------------

# 1 Merge data -------------------------------------------------------------     

ESS = custom_vcat(
    ESS1,
    ESS2,
    ESS2_IT,
    ESS3,
    ESS3_LV,
    ESS3_RO,
    ESS4,
    ESS4_AT,
    ESS4_LT,
    ESS5,
    ESS5_AT,
    ESS6,
    ESS7,
    ESS8,
    ESS9,
    ESS10
)

@transform!(ESS, :pid = 1:nrow(ESS))
@transform!(ESS, :essround = Int.(:essround))

# 2 Migration -------------------------------------------------------------

# Identify migration status: first-gen, second-gen, or native
transform!(ESS,
    [:brncntr, :facntr, :mocntr] =>
        ByRow((r, f, m) -> identify_migration_status(r, f, m)) =>
            :migration)

# 3 Intergenerational mobility indicators ----------------------------------

ESS = @chain ESS begin
    @transform(
        # Highest parents' education
        :edu4_p = [ismissing(m) || ismissing(f) ? missing : (m > f ? m : f) for (m, f) in zip(:edu4_m, :edu4_f)]
    )
    @transform(
        :immobile_absolute = [ismissing(r) || ismissing(p) ? missing : (r != p ? 1 : 0) for (r, p) in zip(:edu4_r, :edu4_p)],
        :mobile_absolute = [ismissing(r) || ismissing(p) ? missing : (r == p ? 1 : 0) for (r, p) in zip(:edu4_r, :edu4_p)],
        :up_absolute = [ismissing(r) || ismissing(p) ? missing : (r > p ? 1 : 0) for (r, p) in zip(:edu4_r, :edu4_p)],
        :down_absolute = [ismissing(r) || ismissing(p) ? missing : (r < p ? 1 : 0) for (r, p) in zip(:edu4_r, :edu4_p)]
    )
end

# 4 Save data -------------------------------------------------------------

Arrow.write("Datasets_tidy/ESS.arrow", ESS)