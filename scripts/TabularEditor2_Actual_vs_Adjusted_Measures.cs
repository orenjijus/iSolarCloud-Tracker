// ============================================
// Tabular Editor 2 C# Script
// Actual vs Adjusted Performance Measures
// ============================================
// Cara menggunakan:
// 1. Buka Tabular Editor 2
// 2. Connect ke PowerBI model
// 3. File > New > C# Script
// 4. Copy paste script ini
// 5. Edit nama tabel di bawah (ganti "YourTableName" dengan nama tabel yang sesuai)
// 6. Run script (F5 atau klik Run)
// ============================================

// ============================================
// KONFIGURASI: GANTI NAMA TABEL DI SINI
// ============================================
string tableName = "Measurement"; // Ganti dengan nama tabel yang sesuai, contoh: "Measurement" atau "mart mart_site_performance_daily"
string factTableName = "mart mart_site_performance_daily"; // Nama tabel fact untuk referensi kolom

// ============================================
// HELPER: Get or Create Table
// ============================================
var table = Model.Tables[tableName];
if (table == null)
{
    Error(string.Format("Table '{0}' tidak ditemukan. Pastikan nama tabel sudah benar.", tableName));
    return;
}

// ============================================
// HELPER MEASURE
// ============================================

// Helper measure untuk check apakah harus exclude issue dates
var helperMeasure = table.AddMeasure("_ShouldExcludeIssueDates");
helperMeasure.Expression = @"
VAR SelectedView = SELECTEDVALUE(Parameter_ViewType[ViewTypeValue], 0)
RETURN
    IF(SelectedView = 1, TRUE(), FALSE())";
helperMeasure.Description = "Helper measure untuk check apakah harus exclude issue dates";

// ============================================
// ENERGY MEASURES
// ============================================

// Energy MWh
var energyMWh = table.AddMeasure("Energy MWh");
energyMWh.Expression = string.Format(@"
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
energyMWh.Description = "Energy (MWh) - Toggle Actual/Adjusted via slicer";
energyMWh.FormatString = "#,##0.00";

// Energy Monthly MWh
var energyMonthlyMWh = table.AddMeasure("Energy Monthly MWh");
energyMonthlyMWh.Expression = string.Format(@"
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
energyMonthlyMWh.Description = "Energy Monthly (MWh) - Toggle Actual/Adjusted via slicer";
energyMonthlyMWh.FormatString = "#,##0.00";

// Energy YTD MWh
var energyYTDMWh = table.AddMeasure("Energy YTD MWh");
energyYTDMWh.Expression = string.Format(@"
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
energyYTDMWh.Description = "Energy YTD (MWh) - Toggle Actual/Adjusted via slicer";
energyYTDMWh.FormatString = "#,##0.00";

// ============================================
// GHI MEASURES
// ============================================

// GHI kWh/m²
var ghiKwhM2 = table.AddMeasure("GHI kWh/m²");
ghiKwhM2.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[daily_ghi_kwh_m2]),
            '{0}'[is_issue_date] = FALSE()
        ),
        AVERAGE('{0}'[daily_ghi_kwh_m2])
    )", factTableName);
ghiKwhM2.Description = "GHI (kWh/m²) - Toggle Actual/Adjusted via slicer";
ghiKwhM2.FormatString = "#,##0.00";

// GHI Monthly Avg kWh/m²
var ghiMonthlyAvg = table.AddMeasure("GHI Monthly Avg kWh/m²");
ghiMonthlyAvg.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[daily_ghi_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('{0}'[daily_ghi_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
ghiMonthlyAvg.Description = "GHI Monthly Average (kWh/m²) - Toggle Actual/Adjusted via slicer";
ghiMonthlyAvg.FormatString = "#,##0.00";

// GHI YTD Avg kWh/m²
var ghiYTDAvg = table.AddMeasure("GHI YTD Avg kWh/m²");
ghiYTDAvg.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[daily_ghi_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('{0}'[daily_ghi_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
ghiYTDAvg.Description = "GHI YTD Average (kWh/m²) - Toggle Actual/Adjusted via slicer";
ghiYTDAvg.FormatString = "#,##0.00";

// ============================================
// POA MEASURES
// ============================================

// POA kWh/m²
var poaKwhM2 = table.AddMeasure("POA kWh/m²");
poaKwhM2.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[is_issue_date] = FALSE()
        ),
        AVERAGE('{0}'[daily_poa_weighted_kwh_m2])
    )", factTableName);
poaKwhM2.Description = "POA (kWh/m²) - Toggle Actual/Adjusted via slicer";
poaKwhM2.FormatString = "#,##0.00";

// POA Monthly Avg kWh/m²
var poaMonthlyAvg = table.AddMeasure("POA Monthly Avg kWh/m²");
poaMonthlyAvg.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
poaMonthlyAvg.Description = "POA Monthly Average (kWh/m²) - Toggle Actual/Adjusted via slicer";
poaMonthlyAvg.FormatString = "#,##0.00";

// POA YTD Avg kWh/m²
var poaYTDAvg = table.AddMeasure("POA YTD Avg kWh/m²");
poaYTDAvg.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('{0}'[daily_poa_weighted_kwh_m2]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
poaYTDAvg.Description = "POA YTD Average (kWh/m²) - Toggle Actual/Adjusted via slicer";
poaYTDAvg.FormatString = "#,##0.00";

// ============================================
// PR MEASURES
// ============================================

// PR GHI
var prGhi = table.AddMeasure("PR GHI");
prGhi.Expression = string.Format(@"
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
prGhi.Description = "PR GHI - Toggle Actual/Adjusted via slicer";
prGhi.FormatString = "0.00%";

// PR GHI Monthly Avg
var prGhiMonthlyAvg = table.AddMeasure("PR GHI Monthly Avg");
prGhiMonthlyAvg.Expression = string.Format(@"
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
prGhiMonthlyAvg.Description = "PR GHI Monthly Average - Toggle Actual/Adjusted via slicer";
prGhiMonthlyAvg.FormatString = "0.00%";

// PR GHI YTD Avg
var prGhiYTDAvg = table.AddMeasure("PR GHI YTD Avg");
prGhiYTDAvg.Expression = string.Format(@"
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
prGhiYTDAvg.Description = "PR GHI YTD Average - Toggle Actual/Adjusted via slicer";
prGhiYTDAvg.FormatString = "0.00%";

// PR POA
var prPoa = table.AddMeasure("PR POA");
prPoa.Expression = string.Format(@"
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
prPoa.Description = "PR POA - Toggle Actual/Adjusted via slicer";
prPoa.FormatString = "0.00%";

// PR POA Monthly Avg
var prPoaMonthlyAvg = table.AddMeasure("PR POA Monthly Avg");
prPoaMonthlyAvg.Expression = string.Format(@"
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
prPoaMonthlyAvg.Description = "PR POA Monthly Average - Toggle Actual/Adjusted via slicer";
prPoaMonthlyAvg.FormatString = "0.00%";

// PR POA YTD Avg
var prPoaYTDAvg = table.AddMeasure("PR POA YTD Avg");
prPoaYTDAvg.Expression = string.Format(@"
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
prPoaYTDAvg.Description = "PR POA YTD Average - Toggle Actual/Adjusted via slicer";
prPoaYTDAvg.FormatString = "0.00%";

// ============================================
// AVAILABILITY MEASURES
// ============================================

// Availability %
var availabilityPct = table.AddMeasure("Availability %");
availabilityPct.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[availability_percent]),
            '{0}'[is_issue_date] = FALSE()
        ),
        AVERAGE('{0}'[availability_percent])
    )", factTableName);
availabilityPct.Description = "Availability (%) - Toggle Actual/Adjusted via slicer";
availabilityPct.FormatString = "0.00%";

// Availability Monthly Avg %
var availabilityMonthlyAvg = table.AddMeasure("Availability Monthly Avg %");
availabilityMonthlyAvg.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[availability_percent]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('{0}'[availability_percent]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
availabilityMonthlyAvg.Description = "Availability Monthly Average (%) - Toggle Actual/Adjusted via slicer";
availabilityMonthlyAvg.FormatString = "0.00%";

// Availability YTD Avg %
var availabilityYTDAvg = table.AddMeasure("Availability YTD Avg %");
availabilityYTDAvg.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[availability_percent]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('{0}'[availability_percent]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
availabilityYTDAvg.Description = "Availability YTD Average (%) - Toggle Actual/Adjusted via slicer";
availabilityYTDAvg.FormatString = "0.00%";

// ============================================
// TARGET COMPARISON MEASURES
// ============================================

// Energy vs Target %
var energyVsTarget = table.AddMeasure("Energy vs Target %");
energyVsTarget.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[energy_actual_vs_target_pct]),
            '{0}'[is_issue_date] = FALSE()
        ),
        AVERAGE('{0}'[energy_actual_vs_target_pct])
    )", factTableName);
energyVsTarget.Description = "Energy vs Target (%) - Toggle Actual/Adjusted via slicer";
energyVsTarget.FormatString = "0.00%";

// Energy vs Target Monthly %
var energyVsTargetMonthly = table.AddMeasure("Energy vs Target Monthly %");
energyVsTargetMonthly.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR CurrentMonth = SELECTEDVALUE('{0}'[month])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[energy_actual_vs_target_pct]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        ),
        CALCULATE(
            AVERAGE('{0}'[energy_actual_vs_target_pct]),
            '{0}'[year] = CurrentYear,
            '{0}'[month] = CurrentMonth
        )
    )", factTableName);
energyVsTargetMonthly.Description = "Energy vs Target Monthly (%) - Toggle Actual/Adjusted via slicer";
energyVsTargetMonthly.FormatString = "0.00%";

// Energy vs Target YTD %
var energyVsTargetYTD = table.AddMeasure("Energy vs Target YTD %");
energyVsTargetYTD.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
VAR CurrentYear = SELECTEDVALUE('{0}'[year])
VAR MaxDate = MAX('{0}'[date_key])
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[energy_actual_vs_target_pct]),
            '{0}'[is_issue_date] = FALSE(),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        ),
        CALCULATE(
            AVERAGE('{0}'[energy_actual_vs_target_pct]),
            '{0}'[year] = CurrentYear,
            '{0}'[date_key] <= MaxDate
        )
    )", factTableName);
energyVsTargetYTD.Description = "Energy vs Target YTD (%) - Toggle Actual/Adjusted via slicer";
energyVsTargetYTD.FormatString = "0.00%";

// GHI vs Target %
var ghiVsTarget = table.AddMeasure("GHI vs Target %");
ghiVsTarget.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[ghi_actual_vs_target_pct]),
            '{0}'[is_issue_date] = FALSE()
        ),
        AVERAGE('{0}'[ghi_actual_vs_target_pct])
    )", factTableName);
ghiVsTarget.Description = "GHI vs Target (%) - Toggle Actual/Adjusted via slicer";
ghiVsTarget.FormatString = "0.00%";

// POA vs Target %
var poaVsTarget = table.AddMeasure("POA vs Target %");
poaVsTarget.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            AVERAGE('{0}'[poa_actual_vs_target_pct]),
            '{0}'[is_issue_date] = FALSE()
        ),
        AVERAGE('{0}'[poa_actual_vs_target_pct])
    )", factTableName);
poaVsTarget.Description = "POA vs Target (%) - Toggle Actual/Adjusted via slicer";
poaVsTarget.FormatString = "0.00%";

// ============================================
// COUNT MEASURES
// ============================================

// Total Days
var totalDays = table.AddMeasure("Total Days");
totalDays.Expression = string.Format(@"
VAR ExcludeIssues = [_ShouldExcludeIssueDates]
RETURN
    IF(
        ExcludeIssues,
        CALCULATE(
            COUNTROWS('{0}'),
            '{0}'[is_issue_date] = FALSE()
        ),
        COUNTROWS('{0}')
    )", factTableName);
totalDays.Description = "Total Days Count - Toggle Actual/Adjusted via slicer";
totalDays.FormatString = "#,##0";

// Issue Days Count
var issueDaysCount = table.AddMeasure("Issue Days Count");
issueDaysCount.Expression = string.Format(@"
COUNTROWS(
    FILTER(
        '{0}',
        '{0}'[is_issue_date] = TRUE()
    )
)", factTableName);
issueDaysCount.Description = "Count of days marked as issue dates";
issueDaysCount.FormatString = "#,##0";

// Adjusted Days Count
var adjustedDaysCount = table.AddMeasure("Adjusted Days Count");
adjustedDaysCount.Expression = string.Format(@"
COUNTROWS(
    FILTER(
        '{0}',
        '{0}'[is_issue_date] = FALSE()
    )
)", factTableName);
adjustedDaysCount.Description = "Count of days that are NOT issue dates";
adjustedDaysCount.FormatString = "#,##0";

// Issue Days %
var issueDaysPct = table.AddMeasure("Issue Days %");
issueDaysPct.Expression = string.Format(@"
DIVIDE(
    [Issue Days Count],
    COUNTROWS('{0}'),
    0
) * 100", factTableName);
issueDaysPct.Description = "Percentage of days that are issue dates";
issueDaysPct.FormatString = "0.00%";

// ============================================
// SUKSES MESSAGE
// ============================================
Info(string.Format("Berhasil membuat {0} measures di tabel '{1}'!", table.Measures.Count, tableName));

