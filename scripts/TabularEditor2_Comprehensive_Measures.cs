// Comprehensive Measures Script for Tabular Editor 2
// Creates all measures for Energy, GHI, POA, PR, and comparisons
// Supports: Daily, Monthly, MTD, Yearly, YTD
// All measures respect the Actual/Adjusted toggle via Parameter_ViewType

var tableName = "Measurement"; // Nama tabel untuk measures (biasanya "Measurement")
var factTableName = "mart mart_site_performance_daily"; // Nama tabel fact untuk referensi kolom di DAX
var factTable = Model.Tables[tableName];

if (factTable == null)
{
    Error(string.Format("Table '{0}' not found. Please update the 'tableName' variable.", tableName));
    return;
}

// Helper measure to check if we should exclude issue dates
var helperMeasure = factTable.AddMeasure("_ShouldExcludeIssueDates");
helperMeasure.Expression = @"
VAR SelectedView = SELECTEDVALUE('Parameter_ViewType'[ViewTypeValue], 0)
RETURN
    IF(SelectedView = 1, TRUE(), FALSE())";
helperMeasure.IsHidden = true;
helperMeasure.Description = "Helper measure to determine if issue dates should be excluded (Adjusted view)";

// ============================================
// ENERGY MEASURES
// ============================================

// Energy Actual - Daily
var energyActualDaily = factTable.AddMeasure("Energy Actual (Daily)");
energyActualDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[is_issue_date] = FALSE()
        ),
        SUM('{0}'[daily_energy_mwh])
    )", factTableName);
energyActualDaily.FormatString = "#,##0.00";
energyActualDaily.Description = "Daily Energy Actual (MWh)";

// Energy Actual - Monthly
var energyActualMonthly = factTable.AddMeasure("Energy Actual (Monthly)");
energyActualMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
energyActualMonthly.FormatString = "#,##0.00";
energyActualMonthly.Description = "Monthly Energy Actual (MWh) - Sum of daily values for selected month";

// Energy Actual - MTD
var energyActualMTD = factTable.AddMeasure("Energy Actual (MTD)");
energyActualMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
energyActualMTD.FormatString = "#,##0.00";
energyActualMTD.Description = "Month-to-Date Energy Actual (MWh)";

// Energy Actual - Yearly
var energyActualYearly = factTable.AddMeasure("Energy Actual (Yearly)");
energyActualYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
energyActualYearly.FormatString = "#,##0.00";
energyActualYearly.Description = "Yearly Energy Actual (MWh) - Sum of daily values for selected year";

// Energy Actual - YTD
var energyActualYTD = factTable.AddMeasure("Energy Actual (YTD)");
energyActualYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            SUM('{0}'[daily_energy_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
energyActualYTD.FormatString = "#,##0.00";
energyActualYTD.Description = "Year-to-Date Energy Actual (MWh)";

// Energy Target - Daily
var energyTargetDaily = factTable.AddMeasure("Energy Target (Daily)");
energyTargetDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[is_issue_date] = FALSE()
        ),
        SUM('{0}'[energy_target_mwh])
    )", factTableName);
energyTargetDaily.FormatString = "#,##0.00";
energyTargetDaily.Description = "Daily Energy Target (MWh)";

// Energy Target - Monthly
var energyTargetMonthly = factTable.AddMeasure("Energy Target (Monthly)");
energyTargetMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
energyTargetMonthly.FormatString = "#,##0.00";
energyTargetMonthly.Description = "Monthly Energy Target (MWh)";

// Energy Target - MTD
var energyTargetMTD = factTable.AddMeasure("Energy Target (MTD)");
energyTargetMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
energyTargetMTD.FormatString = "#,##0.00";
energyTargetMTD.Description = "Month-to-Date Energy Target (MWh)";

// Energy Target - Yearly
var energyTargetYearly = factTable.AddMeasure("Energy Target (Yearly)");
energyTargetYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
energyTargetYearly.FormatString = "#,##0.00";
energyTargetYearly.Description = "Yearly Energy Target (MWh)";

// Energy Target - YTD
var energyTargetYTD = factTable.AddMeasure("Energy Target (YTD)");
energyTargetYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            SUM('{0}'[energy_target_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
energyTargetYTD.FormatString = "#,##0.00";
energyTargetYTD.Description = "Year-to-Date Energy Target (MWh)";

// Energy KPI - Daily (from mart_site_performance_daily table)
var energyKPIDaily = factTable.AddMeasure("Energy KPI (Daily)");
energyKPIDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[is_issue_date] = FALSE()
        ),
        SUM('{0}'[energy_kpi_daily_mwh])
    )", factTableName);
energyKPIDaily.FormatString = "#,##0.00";
energyKPIDaily.Description = "Daily Energy KPI (MWh)";

// Energy KPI - Monthly (from mart_site_performance_daily table)
var energyKPIMonthly = factTable.AddMeasure("Energy KPI (Monthly)");
energyKPIMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
energyKPIMonthly.FormatString = "#,##0.00";
energyKPIMonthly.Description = "Monthly Energy KPI (MWh) - Sum of daily KPI for selected month";

// Energy KPI - MTD
var energyKPIMTD = factTable.AddMeasure("Energy KPI (MTD)");
energyKPIMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
energyKPIMTD.FormatString = "#,##0.00";
energyKPIMTD.Description = "Month-to-Date Energy KPI (MWh)";

// Energy KPI - Yearly
var energyKPIYearly = factTable.AddMeasure("Energy KPI (Yearly)");
energyKPIYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
energyKPIYearly.FormatString = "#,##0.00";
energyKPIYearly.Description = "Yearly Energy KPI (MWh) - Sum of daily KPI for selected year";

// Energy KPI - YTD
var energyKPIYTD = factTable.AddMeasure("Energy KPI (YTD)");
energyKPIYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            SUM('{0}'[energy_kpi_daily_mwh]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
energyKPIYTD.FormatString = "#,##0.00";
energyKPIYTD.Description = "Year-to-Date Energy KPI (MWh)";

// Energy Actual/Target - Daily
var energyActualTargetDaily = factTable.AddMeasure("Energy Actual/Target (Daily)");
energyActualTargetDaily.Expression = @"IF(
    [Energy Target (Daily)] > 0,
    DIVIDE([Energy Actual (Daily)], [Energy Target (Daily)]),
    BLANK()
)";
energyActualTargetDaily.FormatString = "0.00%";
energyActualTargetDaily.Description = "Daily Energy Actual vs Target Ratio";

// Energy Actual/Target - Monthly
var energyActualTargetMonthly = factTable.AddMeasure("Energy Actual/Target (Monthly)");
energyActualTargetMonthly.Expression = string.Format(@"IF(
    [Energy Target (Monthly)] > 0,
    DIVIDE([Energy Actual (Monthly)], [Energy Target (Monthly)]),
    BLANK()
)");
energyActualTargetMonthly.FormatString = "0.00%";
energyActualTargetMonthly.Description = "Monthly Energy Actual vs Target Ratio";

// Energy Actual/Target - MTD
var energyActualTargetMTD = factTable.AddMeasure("Energy Actual/Target (MTD)");
energyActualTargetMTD.Expression = string.Format(@"IF(
    [Energy Target (MTD)] > 0,
    DIVIDE([Energy Actual (MTD)], [Energy Target (MTD)]),
    BLANK()
)");
energyActualTargetMTD.FormatString = "0.00%";
energyActualTargetMTD.Description = "Month-to-Date Energy Actual vs Target Ratio";

// Energy Actual/Target - Yearly
var energyActualTargetYearly = factTable.AddMeasure("Energy Actual/Target (Yearly)");
energyActualTargetYearly.Expression = string.Format(@"IF(
    [Energy Target (Yearly)] > 0,
    DIVIDE([Energy Actual (Yearly)], [Energy Target (Yearly)]),
    BLANK()
)");
energyActualTargetYearly.FormatString = "0.00%";
energyActualTargetYearly.Description = "Yearly Energy Actual vs Target Ratio";

// Energy Actual/Target - YTD
var energyActualTargetYTD = factTable.AddMeasure("Energy Actual/Target (YTD)");
energyActualTargetYTD.Expression = string.Format(@"IF(
    [Energy Target (YTD)] > 0,
    DIVIDE([Energy Actual (YTD)], [Energy Target (YTD)]),
    BLANK()
)");
energyActualTargetYTD.FormatString = "0.00%";
energyActualTargetYTD.Description = "Year-to-Date Energy Actual vs Target Ratio";

// Energy Actual/KPI - Daily
var energyActualKPIDaily = factTable.AddMeasure("Energy Actual/KPI (Daily)");
energyActualKPIDaily.Expression = @"IF(
    [Energy KPI (Daily)] > 0,
    DIVIDE([Energy Actual (Daily)], [Energy KPI (Daily)]),
    BLANK()
)";
energyActualKPIDaily.FormatString = "0.00%";
energyActualKPIDaily.Description = "Daily Energy Actual vs KPI Ratio";

// Energy Actual/KPI - Monthly
var energyActualKPIMonthly = factTable.AddMeasure("Energy Actual/KPI (Monthly)");
energyActualKPIMonthly.Expression = @"IF(
    [Energy KPI (Monthly)] > 0,
    DIVIDE([Energy Actual (Monthly)], [Energy KPI (Monthly)]),
    BLANK()
)";
energyActualKPIMonthly.FormatString = "0.00%";
energyActualKPIMonthly.Description = "Monthly Energy Actual vs KPI Ratio";

// Energy Actual/KPI - MTD
var energyActualKPIMTD = factTable.AddMeasure("Energy Actual/KPI (MTD)");
energyActualKPIMTD.Expression = @"IF(
    [Energy KPI (MTD)] > 0,
    DIVIDE([Energy Actual (MTD)], [Energy KPI (MTD)]),
    BLANK()
)";
energyActualKPIMTD.FormatString = "0.00%";
energyActualKPIMTD.Description = "Month-to-Date Energy Actual vs KPI Ratio";

// Energy Actual/KPI - Yearly
var energyActualKPIYearly = factTable.AddMeasure("Energy Actual/KPI (Yearly)");
energyActualKPIYearly.Expression = @"IF(
    [Energy KPI (Yearly)] > 0,
    DIVIDE([Energy Actual (Yearly)], [Energy KPI (Yearly)]),
    BLANK()
)";
energyActualKPIYearly.FormatString = "0.00%";
energyActualKPIYearly.Description = "Yearly Energy Actual vs KPI Ratio";

// Energy Actual/KPI - YTD
var energyActualKPIYTD = factTable.AddMeasure("Energy Actual/KPI (YTD)");
energyActualKPIYTD.Expression = @"IF(
    [Energy KPI (YTD)] > 0,
    DIVIDE([Energy Actual (YTD)], [Energy KPI (YTD)]),
    BLANK()
)";
energyActualKPIYTD.FormatString = "0.00%";
energyActualKPIYTD.Description = "Year-to-Date Energy Actual vs KPI Ratio";

// ============================================
// GHI MEASURES
// ============================================

// GHI Actual - Daily
var ghiActualDaily = factTable.AddMeasure("GHI Actual (Daily)");
ghiActualDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[is_issue_date] = FALSE()
        ),
        SUM('{0}'[daily_ghi_kwh_m2])
    )", factTableName);
ghiActualDaily.FormatString = "#,##0.00";
ghiActualDaily.Description = "Daily GHI Actual (kWh/m²)";

// GHI Actual - Monthly
var ghiActualMonthly = factTable.AddMeasure("GHI Actual (Monthly)");
ghiActualMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
ghiActualMonthly.FormatString = "#,##0.00";
ghiActualMonthly.Description = "Monthly GHI Actual (kWh/m²)";

// GHI Actual - MTD
var ghiActualMTD = factTable.AddMeasure("GHI Actual (MTD)");
ghiActualMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
ghiActualMTD.FormatString = "#,##0.00";
ghiActualMTD.Description = "Month-to-Date GHI Actual (kWh/m²)";

// GHI Actual - Yearly
var ghiActualYearly = factTable.AddMeasure("GHI Actual (Yearly)");
ghiActualYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
ghiActualYearly.FormatString = "#,##0.00";
ghiActualYearly.Description = "Yearly GHI Actual (kWh/m²)";

// GHI Actual - YTD
var ghiActualYTD = factTable.AddMeasure("GHI Actual (YTD)");
ghiActualYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            SUM('{0}'[daily_ghi_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
ghiActualYTD.FormatString = "#,##0.00";
ghiActualYTD.Description = "Year-to-Date GHI Actual (kWh/m²)";

// GHI Target - Daily
var ghiTargetDaily = factTable.AddMeasure("GHI Target (Daily)");
ghiTargetDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[is_issue_date] = FALSE()
        ),
        SUM('{0}'[ghi_target])
    )", factTableName);
ghiTargetDaily.FormatString = "#,##0.00";
ghiTargetDaily.Description = "Daily GHI Target (kWh/m²)";

// GHI Target - Monthly
var ghiTargetMonthly = factTable.AddMeasure("GHI Target (Monthly)");
ghiTargetMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
ghiTargetMonthly.FormatString = "#,##0.00";
ghiTargetMonthly.Description = "Monthly GHI Target (kWh/m²)";

// GHI Target - MTD
var ghiTargetMTD = factTable.AddMeasure("GHI Target (MTD)");
ghiTargetMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
ghiTargetMTD.FormatString = "#,##0.00";
ghiTargetMTD.Description = "Month-to-Date GHI Target (kWh/m²)";

// GHI Target - Yearly
var ghiTargetYearly = factTable.AddMeasure("GHI Target (Yearly)");
ghiTargetYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
ghiTargetYearly.FormatString = "#,##0.00";
ghiTargetYearly.Description = "Yearly GHI Target (kWh/m²)";

// GHI Target - YTD
var ghiTargetYTD = factTable.AddMeasure("GHI Target (YTD)");
ghiTargetYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            SUM('{0}'[ghi_target]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
ghiTargetYTD.FormatString = "#,##0.00";
ghiTargetYTD.Description = "Year-to-Date GHI Target (kWh/m²)";

// GHI Actual/Target - Daily
var ghiActualTargetDaily = factTable.AddMeasure("GHI Actual/Target (Daily)");
ghiActualTargetDaily.Expression = string.Format(@"IF(
    [GHI Target (Daily)] > 0,
    DIVIDE([GHI Actual (Daily)], [GHI Target (Daily)]),
    BLANK()
)");
ghiActualTargetDaily.FormatString = "0.00%";
ghiActualTargetDaily.Description = "Daily GHI Actual vs Target Ratio";

// GHI Actual/Target - Monthly
var ghiActualTargetMonthly = factTable.AddMeasure("GHI Actual/Target (Monthly)");
ghiActualTargetMonthly.Expression = string.Format(@"IF(
    [GHI Target (Monthly)] > 0,
    DIVIDE([GHI Actual (Monthly)], [GHI Target (Monthly)]),
    BLANK()
)");
ghiActualTargetMonthly.FormatString = "0.00%";
ghiActualTargetMonthly.Description = "Monthly GHI Actual vs Target Ratio";

// GHI Actual/Target - MTD
var ghiActualTargetMTD = factTable.AddMeasure("GHI Actual/Target (MTD)");
ghiActualTargetMTD.Expression = string.Format(@"IF(
    [GHI Target (MTD)] > 0,
    DIVIDE([GHI Actual (MTD)], [GHI Target (MTD)]),
    BLANK()
)");
ghiActualTargetMTD.FormatString = "0.00%";
ghiActualTargetMTD.Description = "Month-to-Date GHI Actual vs Target Ratio";

// GHI Actual/Target - Yearly
var ghiActualTargetYearly = factTable.AddMeasure("GHI Actual/Target (Yearly)");
ghiActualTargetYearly.Expression = string.Format(@"IF(
    [GHI Target (Yearly)] > 0,
    DIVIDE([GHI Actual (Yearly)], [GHI Target (Yearly)]),
    BLANK()
)");
ghiActualTargetYearly.FormatString = "0.00%";
ghiActualTargetYearly.Description = "Yearly GHI Actual vs Target Ratio";

// GHI Actual/Target - YTD
var ghiActualTargetYTD = factTable.AddMeasure("GHI Actual/Target (YTD)");
ghiActualTargetYTD.Expression = string.Format(@"IF(
    [GHI Target (YTD)] > 0,
    DIVIDE([GHI Actual (YTD)], [GHI Target (YTD)]),
    BLANK()
)");
ghiActualTargetYTD.FormatString = "0.00%";
ghiActualTargetYTD.Description = "Year-to-Date GHI Actual vs Target Ratio";

// ============================================
// POA MEASURES
// ============================================

// POA Actual - Daily
var poaActualDaily = factTable.AddMeasure("POA Actual (Daily)");
poaActualDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[is_issue_date] = FALSE()
        ),
        SUM('{0}'[daily_poa_weighted_kwh_m2])
    )", factTableName);
poaActualDaily.FormatString = "#,##0.00";
poaActualDaily.Description = "Daily POA Actual (kWh/m²)";

// POA Actual - Monthly
var poaActualMonthly = factTable.AddMeasure("POA Actual (Monthly)");
poaActualMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
poaActualMonthly.FormatString = "#,##0.00";
poaActualMonthly.Description = "Monthly POA Actual (kWh/m²)";

// POA Actual - MTD
var poaActualMTD = factTable.AddMeasure("POA Actual (MTD)");
poaActualMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
poaActualMTD.FormatString = "#,##0.00";
poaActualMTD.Description = "Month-to-Date POA Actual (kWh/m²)";

// POA Actual - Yearly
var poaActualYearly = factTable.AddMeasure("POA Actual (Yearly)");
poaActualYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
poaActualYearly.FormatString = "#,##0.00";
poaActualYearly.Description = "Yearly POA Actual (kWh/m²)";

// POA Actual - YTD
var poaActualYTD = factTable.AddMeasure("POA Actual (YTD)");
poaActualYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            SUM('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
poaActualYTD.FormatString = "#,##0.00";
poaActualYTD.Description = "Year-to-Date POA Actual (kWh/m²)";

// POA Target - Daily
var poaTargetDaily = factTable.AddMeasure("POA Target (Daily)");
poaTargetDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[is_issue_date] = FALSE()
        ),
        SUM('{0}'[poa_target])
    )", factTableName);
poaTargetDaily.FormatString = "#,##0.00";
poaTargetDaily.Description = "Daily POA Target (kWh/m²)";

// POA Target - Monthly
var poaTargetMonthly = factTable.AddMeasure("POA Target (Monthly)");
poaTargetMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
poaTargetMonthly.FormatString = "#,##0.00";
poaTargetMonthly.Description = "Monthly POA Target (kWh/m²)";

// POA Target - MTD
var poaTargetMTD = factTable.AddMeasure("POA Target (MTD)");
poaTargetMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
poaTargetMTD.FormatString = "#,##0.00";
poaTargetMTD.Description = "Month-to-Date POA Target (kWh/m²)";

// POA Target - Yearly
var poaTargetYearly = factTable.AddMeasure("POA Target (Yearly)");
poaTargetYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
poaTargetYearly.FormatString = "#,##0.00";
poaTargetYearly.Description = "Yearly POA Target (kWh/m²)";

// POA Target - YTD
var poaTargetYTD = factTable.AddMeasure("POA Target (YTD)");
poaTargetYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            SUM('{0}'[poa_target]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
poaTargetYTD.FormatString = "#,##0.00";
poaTargetYTD.Description = "Year-to-Date POA Target (kWh/m²)";

// POA Actual/Target - Daily
var poaActualTargetDaily = factTable.AddMeasure("POA Actual/Target (Daily)");
poaActualTargetDaily.Expression = string.Format(@"IF(
    [POA Target (Daily)] > 0,
    DIVIDE([POA Actual (Daily)], [POA Target (Daily)]),
    BLANK()
)");
poaActualTargetDaily.FormatString = "0.00%";
poaActualTargetDaily.Description = "Daily POA Actual vs Target Ratio";

// POA Actual/Target - Monthly
var poaActualTargetMonthly = factTable.AddMeasure("POA Actual/Target (Monthly)");
poaActualTargetMonthly.Expression = string.Format(@"IF(
    [POA Target (Monthly)] > 0,
    DIVIDE([POA Actual (Monthly)], [POA Target (Monthly)]),
    BLANK()
)");
poaActualTargetMonthly.FormatString = "0.00%";
poaActualTargetMonthly.Description = "Monthly POA Actual vs Target Ratio";

// POA Actual/Target - MTD
var poaActualTargetMTD = factTable.AddMeasure("POA Actual/Target (MTD)");
poaActualTargetMTD.Expression = string.Format(@"IF(
    [POA Target (MTD)] > 0,
    DIVIDE([POA Actual (MTD)], [POA Target (MTD)]),
    BLANK()
)");
poaActualTargetMTD.FormatString = "0.00%";
poaActualTargetMTD.Description = "Month-to-Date POA Actual vs Target Ratio";

// POA Actual/Target - Yearly
var poaActualTargetYearly = factTable.AddMeasure("POA Actual/Target (Yearly)");
poaActualTargetYearly.Expression = string.Format(@"IF(
    [POA Target (Yearly)] > 0,
    DIVIDE([POA Actual (Yearly)], [POA Target (Yearly)]),
    BLANK()
)");
poaActualTargetYearly.FormatString = "0.00%";
poaActualTargetYearly.Description = "Yearly POA Actual vs Target Ratio";

// POA Actual/Target - YTD
var poaActualTargetYTD = factTable.AddMeasure("POA Actual/Target (YTD)");
poaActualTargetYTD.Expression = string.Format(@"IF(
    [POA Target (YTD)] > 0,
    DIVIDE([POA Actual (YTD)], [POA Target (YTD)]),
    BLANK()
)");
poaActualTargetYTD.FormatString = "0.00%";
poaActualTargetYTD.Description = "Year-to-Date POA Actual vs Target Ratio";

// ============================================
// GHI vs ENERGY (Selisih)
// ============================================
// Formula: Energy Actual/KPI - GHI Actual/Target

// GHI vs Energy - Monthly
var ghiVsEnergyMonthly = factTable.AddMeasure("GHI vs Energy (Monthly)");
ghiVsEnergyMonthly.Expression = @"IF(
    NOT(ISBLANK([Energy Actual/KPI (Monthly)]) || ISBLANK([GHI Actual/Target (Monthly)])),
    [Energy Actual/KPI (Monthly)] - [GHI Actual/Target (Monthly)],
    BLANK()
)";
ghiVsEnergyMonthly.FormatString = "0.00%";
ghiVsEnergyMonthly.Description = "Monthly: Energy Actual/KPI - GHI Actual/Target (Selisih)";

// GHI vs Energy - MTD
var ghiVsEnergyMTD = factTable.AddMeasure("GHI vs Energy (MTD)");
ghiVsEnergyMTD.Expression = @"IF(
    NOT(ISBLANK([Energy Actual/KPI (MTD)]) || ISBLANK([GHI Actual/Target (MTD)])),
    [Energy Actual/KPI (MTD)] - [GHI Actual/Target (MTD)],
    BLANK()
)";
ghiVsEnergyMTD.FormatString = "0.00%";
ghiVsEnergyMTD.Description = "Month-to-Date: Energy Actual/KPI - GHI Actual/Target (Selisih)";

// GHI vs Energy - Yearly
var ghiVsEnergyYearly = factTable.AddMeasure("GHI vs Energy (Yearly)");
ghiVsEnergyYearly.Expression = @"IF(
    NOT(ISBLANK([Energy Actual/KPI (Yearly)]) || ISBLANK([GHI Actual/Target (Yearly)])),
    [Energy Actual/KPI (Yearly)] - [GHI Actual/Target (Yearly)],
    BLANK()
)";
ghiVsEnergyYearly.FormatString = "0.00%";
ghiVsEnergyYearly.Description = "Yearly: Energy Actual/KPI - GHI Actual/Target (Selisih)";

// GHI vs Energy - YTD
var ghiVsEnergyYTD = factTable.AddMeasure("GHI vs Energy (YTD)");
ghiVsEnergyYTD.Expression = @"IF(
    NOT(ISBLANK([Energy Actual/KPI (YTD)]) || ISBLANK([GHI Actual/Target (YTD)])),
    [Energy Actual/KPI (YTD)] - [GHI Actual/Target (YTD)],
    BLANK()
)";
ghiVsEnergyYTD.FormatString = "0.00%";
ghiVsEnergyYTD.Description = "Year-to-Date: Energy Actual/KPI - GHI Actual/Target (Selisih)";

// ============================================
// PR MEASURES
// ============================================

// PR GHI - Daily
var prGhiDaily = factTable.AddMeasure("PR GHI (Daily)");
prGhiDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[is_issue_date] = FALSE()
        ),
        AVERAGE('{0}'[pr_ghi_actual])
    )", factTableName);
prGhiDaily.FormatString = "0.00%";
prGhiDaily.Description = "Daily PR GHI (Performance Ratio)";

// PR GHI - Monthly
var prGhiMonthly = factTable.AddMeasure("PR GHI (Monthly)");
prGhiMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
prGhiMonthly.FormatString = "0.00%";
prGhiMonthly.Description = "Monthly PR GHI (Performance Ratio)";

// PR GHI - MTD
var prGhiMTD = factTable.AddMeasure("PR GHI (MTD)");
prGhiMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
prGhiMTD.FormatString = "0.00%";
prGhiMTD.Description = "Month-to-Date PR GHI (Performance Ratio)";

// PR GHI - Yearly
var prGhiYearly = factTable.AddMeasure("PR GHI (Yearly)");
prGhiYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
prGhiYearly.FormatString = "0.00%";
prGhiYearly.Description = "Yearly PR GHI (Performance Ratio)";

// PR GHI - YTD
var prGhiYTD = factTable.AddMeasure("PR GHI (YTD)");
prGhiYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('{0}'[pr_ghi_actual]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
prGhiYTD.FormatString = "0.00%";
prGhiYTD.Description = "Year-to-Date PR GHI (Performance Ratio)";

// PR POA - Daily
var prPoaDaily = factTable.AddMeasure("PR POA (Daily)");
prPoaDaily.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[is_issue_date] = FALSE()
        ),
        AVERAGE('{0}'[pr_poa_actual])
    )", factTableName);
prPoaDaily.FormatString = "0.00%";
prPoaDaily.Description = "Daily PR POA (Performance Ratio)";

// PR POA - Monthly
var prPoaMonthly = factTable.AddMeasure("PR POA (Monthly)");
prPoaMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
prPoaMonthly.FormatString = "0.00%";
prPoaMonthly.Description = "Monthly PR POA (Performance Ratio)";

// PR POA - MTD
var prPoaMTD = factTable.AddMeasure("PR POA (MTD)");
prPoaMTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        ),
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate,
            MONTH('{0}'[date_key]) = MONTH(MaxDate)
        )
    )", factTableName);
prPoaMTD.FormatString = "0.00%";
prPoaMTD.Description = "Month-to-Date PR POA (Performance Ratio)";

// PR POA - Yearly
var prPoaYearly = factTable.AddMeasure("PR POA (Yearly)");
prPoaYearly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear
        ),
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[year] = CurrentYear
        )
    )", factTableName);
prPoaYearly.FormatString = "0.00%";
prPoaYearly.Description = "Yearly PR POA (Performance Ratio)";

// PR POA - YTD
var prPoaYTD = factTable.AddMeasure("PR POA (YTD)");
prPoaYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('{0}'[pr_poa_actual]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
prPoaYTD.FormatString = "0.00%";
prPoaYTD.Description = "Year-to-Date PR POA (Performance Ratio)";

Info("All measures created successfully!");

