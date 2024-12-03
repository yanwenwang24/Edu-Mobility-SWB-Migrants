## ------------------------------------------------------------------------
##
## Script name: functions.jl
## Purpose: Functions for cleaning and analyzing data
## Author: Yanwen Wang
## Date Created: 2024-12-03
## Email: yanwenwang@u.nus.edu
##
## ------------------------------------------------------------------------
##
## Notes:
##
## ------------------------------------------------------------------------

# Function to vertically concatenate DataFrames with different columns
function custom_vcat(dfs::Vector{DataFrame})
    # Get all unique column names across all DataFrames
    all_columns = unique(vcat([names(df) for df in dfs]...))
    
    # Process each DataFrame to include all columns
    processed_dfs = DataFrame[]
    for df in dfs
        # Create a new DataFrame with missing values for missing columns
        new_df = DataFrame()
        
        # Process each required column
        for col in all_columns
            if col in names(df)
                # If column exists in original DataFrame, copy it
                new_df[!, col] = df[!, col]
            else
                # If column doesn't exist, create it with missing values
                new_df[!, col] = fill(missing, nrow(df))
            end
        end
        
        push!(processed_dfs, new_df)
    end
    
    # Perform vertical concatenation on processed DataFrames
    return vcat(processed_dfs...)
end

# Alternative version that accepts multiple DataFrames as arguments
function custom_vcat(df1::DataFrame, df2::DataFrame, dfs::DataFrame...)
    return custom_vcat(vcat([df1, df2], collect(dfs)))
end

# Function to identify migration status 
function identify_migration_status(brncntr::Union{Float64,Missing},
    facntr::Union{Float64,Missing},
    mocntr::Union{Float64,Missing})
    # Return missing if any required value is missing
    if ismissing(brncntr)
        return missing
    end

    # Case: Both parents' birthplace unknown
    if ismissing(facntr) && ismissing(mocntr)
        return missing
    end

    # Case: Known respondent and parents' status
    if !ismissing(facntr) && !ismissing(mocntr)
        if facntr == 1 && mocntr == 1
            return "native"
        elseif brncntr == 2
            return "first-gen"
        elseif brncntr == 1 && (facntr == 2 || mocntr == 2)
            return "second-gen"
        else
            return "native"
        end
    end

    # Case: Known respondent status, partial parents' status
    # Handle cases with partial parental information
    if brncntr == 2
        # Check if the known parent is native-born
        if (!ismissing(facntr) && facntr == 1) ||
           (!ismissing(mocntr) && mocntr == 1)
            return missing  # Cannot determine without both parents' information
        else
            return "first-gen"
        end
    elseif brncntr == 1
        if (!ismissing(facntr) && facntr == 2) ||
           (!ismissing(mocntr) && mocntr == 2)
            return "second-gen"
        else
            return missing  # Cannot determine without both parents' information
        end
    end

    return missing
end

# Function for analyzing missing values
function analyze_missing_values(df::DataFrame)
    # Calculate missing values for each column
    n_rows = nrow(df)

    # Create a new DataFrame to store results
    missing_stats = DataFrame(
        variable=names(df),
        n_missing=map(col -> sum(ismissing.(col)), eachcol(df)),
        pct_missing=map(col -> round(100 * sum(ismissing.(col)) / n_rows, digits=2), eachcol(df))
    )

    # Sort by percentage missing in descending order
    sort!(missing_stats, :pct_missing, rev=true)

    # Print summary statistics
    n_complete_cols = sum(missing_stats.n_missing .== 0)
    n_partial_cols = sum(missing_stats.n_missing .> 0)
    n_all_missing = sum(missing_stats.n_missing .== n_rows)

    println("\nMissing Values Summary:")
    println("Total number of observations: ", n_rows)
    println("Number of variables: ", ncol(df))
    println("Complete columns (no missing): ", n_complete_cols)
    println("Columns with missing values: ", n_partial_cols)
    println("Columns with all missing: ", n_all_missing)
    println("\nDetailed missing values by column:")

    return missing_stats
end

# Function for spreading column to dummy variables
function spread_to_dummies(df::DataFrame, col::Symbol;
    prefix::Union{String,Nothing}=nothing,
    drop_original::Bool=false)

    # Get the unique values, excluding missing
    unique_vals = sort(unique(skipmissing(df[!, col])))

    # Create prefix for new column names
    # If no prefix provided, use the original column name
    col_prefix = isnothing(prefix) ? String(col) : prefix

    # Generate new column names
    new_cols = [Symbol(col_prefix, "_", val) for val in unique_vals]

    # Create dummy variables
    dummy_cols = Dict(
        new_col => [ismissing(val) ? missing : (val == unique_vals[i] ? 1 : 0)
                    for val in df[!, col]]
        for (i, new_col) in enumerate(new_cols)
    )

    # Create new DataFrame with dummy columns
    result_df = if drop_original
        select(df, Not(col))
    else
        copy(df)
    end

    # Add dummy columns to result
    for (col_name, values) in dummy_cols
        result_df[!, col_name] = values
    end

    return result_df
end

# Function for describing continuous variables
function analyze_variables(df::DataFrame,
    vars::Union{Vector{Symbol},Vector{String}};
    weights::Union{Symbol,String,Nothing}=nothing,
    digits::Int=2)
    # Convert all column names to symbols for consistent handling
    df_names = Symbol.(names(df))
    vars_symbols = Symbol.(vars)
    weights_symbol = isnothing(weights) ? nothing : Symbol(weights)

    # Function to check if a vector is numeric
    function is_numeric(x)
        # Remove missing values for the check
        x_clean = skipmissing(x)

        # Check if numeric (including integers)
        return all(x -> typeof(x) <: Number, x_clean)
    end

    # Check if all specified variables exist in the DataFrame
    missing_vars = setdiff(vars_symbols, df_names)
    if !isempty(missing_vars)
        error("The following variables were not found in the DataFrame: ", join(missing_vars, ", "))
    end

    # Check if weight variable exists when specified
    if !isnothing(weights_symbol) && !(weights_symbol in df_names)
        error("Weight variable ':$weights_symbol' not found in the DataFrame")
    end

    # Check if variables are numeric and store non-numeric ones
    non_numeric = Symbol[]
    for var in vars_symbols
        if !is_numeric(df[!, var])
            push!(non_numeric, var)
        end
    end

    if !isempty(non_numeric)
        error("The following variables are not numeric: ", join(non_numeric, ", "))
    end

    # Initialize results DataFrame
    results = DataFrame(
        variable=String[],
        n=Int[],
        mean=Float64[],
        sd=Float64[],
        min=Float64[],
        p25=Float64[],
        p50=Float64[],
        p75=Float64[],
        max=Float64[]
    )

    for var in vars_symbols
        # Handle missing values
        if isnothing(weights_symbol)
            valid_mask = completecases(df, [var])
        else
            valid_mask = completecases(df, [var, weights_symbol])
        end

        valid_data = df[valid_mask, :]
        x = Float64.(valid_data[!, var])  # Convert to Float64 for consistent calculations

        if isnothing(weights_symbol)
            # Unweighted calculations
            mean_val = mean(x)
            sd_val = std(x)
            quants = quantile(x, [0, 0.25, 0.50, 0.75, 1])
        else
            # Weighted calculations
            w = valid_data[!, weights_symbol]

            # Weighted mean
            mean_val = sum(w .* x) / sum(w)

            # Weighted standard deviation
            weighted_var = sum(w .* (x .- mean_val) .^ 2) / (sum(w) - 1)
            sd_val = sqrt(weighted_var)

            # Quantiles (unweighted for simplicity)
            quants = quantile(x, [0, 0.25, 0.50, 0.75, 1])
        end

        # Add results to DataFrame
        push!(results, (
            variable=String(var),
            n=nrow(valid_data),
            mean=round(mean_val, digits=digits),
            sd=round(sd_val, digits=digits),
            min=round(quants[1], digits=digits),
            p25=round(quants[2], digits=digits),
            p50=round(quants[3], digits=digits),
            p75=round(quants[4], digits=digits),
            max=round(quants[5], digits=digits)
        ))
    end

    # Add information about weights to the column names if weights were used
    if !isnothing(weights_symbol)
        rename!(results,
            :mean => :weighted_mean,
            :sd => :weighted_sd
        )
    end

    return results
end

# Functions for weighted t-tests
function weighted_ttest(x1, x2, w1, w2; digits::Int=3)
    # Calculate weighted means
    μ1 = mean(x1, weights(w1))
    μ2 = mean(x2, weights(w2))

    # Calculate weighted variances
    var1 = var(x1, weights(w1))
    var2 = var(x2, weights(w2))

    # Calculate effective sample sizes
    n1 = sum(w1)^2 / sum(w1 .^ 2)
    n2 = sum(w2)^2 / sum(w2 .^ 2)

    # Calculate t-statistic
    t_stat = (μ1 - μ2) / sqrt(var1 / n1 + var2 / n2)

    # Calculate degrees of freedom (Welch-Satterthwaite equation)
    df = (var1 / n1 + var2 / n2)^2 / ((var1 / n1)^2 / (n1 - 1) + (var2 / n2)^2 / (n2 - 1))

    # Calculate two-sided p-value
    p_value = 2 * (1 - cdf(TDist(df), abs(t_stat)))

    # Store results
    results = (
        t_statistic=t_stat,
        df=df,
        p_value=p_value,
        mean_diff=μ1 - μ2,
        mean1=μ1,
        mean2=μ2,
        sd1=sqrt(var1),
        sd2=sqrt(var2),
        n1=n1,
        n2=n2
    )

    # Print formatted results
    println("\nWeighted T-Test Results")
    println("══════════════════════\n")

    # Group statistics
    println("Group Statistics:")
    println("────────────────")
    println("Group 1: mean = $(round(μ1, digits=digits)), sd = $(round(sqrt(var1), digits=digits)), n = $(round(n1, digits=1))")
    println("Group 2: mean = $(round(μ2, digits=digits)), sd = $(round(sqrt(var2), digits=digits)), n = $(round(n2, digits=1))")

    # Mean difference
    println("\nMean Difference:")
    println("───────────────")
    println("$(round(μ1 - μ2, digits=digits)) (Group 1 - Group 2)")

    # Test statistics
    println("\nTest Statistics:")
    println("───────────────")
    println("t-statistic = $(round(t_stat, digits=digits))")
    println("df = $(round(df, digits=digits))")

    # P-value with stars for significance levels
    sig_stars = if p_value < 0.001
        "***"
    elseif p_value < 0.01
        "**"
    elseif p_value < 0.05
        "*"
    elseif p_value < 0.1
        "†"
    else
        ""
    end

    println("p-value = $(round(p_value, digits=digits))$sig_stars")

    # Significance level legend if stars were used
    if !isempty(sig_stars)
        println("\nSignificance levels:")
        println("───────────────────")
        println("† p < 0.1, * p < 0.05, ** p < 0.01, *** p < 0.001")
    end

    # Return results silently for further use if needed
    return results
end