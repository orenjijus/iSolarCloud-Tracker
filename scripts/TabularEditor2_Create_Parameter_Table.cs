// ============================================
// Tabular Editor 2 C# Script
// Create Parameter_ViewType Table
// ============================================
// Script ini akan membuat calculated table Parameter_ViewType
// yang diperlukan untuk slicer Actual vs Adjusted
// ============================================

// Nama tabel yang akan dibuat
string tableName = "Parameter_ViewType";

// Cek apakah tabel sudah ada
var existingTable = Model.Tables[tableName];
if (existingTable != null)
{
    Info(string.Format("Tabel '{0}' sudah ada. Melewati pembuatan tabel.", tableName));
}
else
{
    // Buat calculated table baru
    var paramTable = Model.AddCalculatedTable(tableName);
    
    // Set expression untuk calculated table
    paramTable.Expression = @"
DATATABLE(
    ""ViewType"", STRING,
    ""ViewTypeValue"", INTEGER,
    {
        {""Actual"", 0},
        {""Adjusted"", 1}
    }
)";
    
    // Set description
    paramTable.Description = "Parameter table untuk toggle Actual vs Adjusted performance via slicer";
    
    // Set format untuk kolom ViewTypeValue (optional)
    var viewTypeValueColumn = paramTable.Columns["ViewTypeValue"];
    if (viewTypeValueColumn != null)
    {
        viewTypeValueColumn.FormatString = "#,##0";
    }
    
    Info(string.Format("Berhasil membuat tabel '{0}'!", tableName));
}

// Verifikasi tabel sudah dibuat
var verifyTable = Model.Tables[tableName];
if (verifyTable != null)
{
    Info(string.Format("Tabel '{0}' sudah tersedia di model.", tableName));
    Info("Kolom yang tersedia:");
    foreach (var col in verifyTable.Columns)
    {
        Info(string.Format("  - {0} ({1})", col.Name, col.DataType));
    }
}
else
{
    Error(string.Format("Gagal membuat tabel '{0}'. Silakan cek error di atas.", tableName));
}

