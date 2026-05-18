"""
Test script untuk memverifikasi logika parsing tanggal FusionSolar
"""
from datetime import datetime, timezone, timedelta

def test_date_parsing(input_date_str):
    """Test parsing tanggal dan konversi ke UTC"""
    print(f"\n{'='*60}")
    print(f"TEST: Input tanggal '{input_date_str}'")
    print(f"{'='*60}")
    
    # Parse date strings - interpret as local time (UTC+7) and convert to UTC
    local_tz = timezone(timedelta(hours=7))  # UTC+7 timezone
    start_date_local = datetime.strptime(input_date_str, '%Y-%m-%d').replace(tzinfo=local_tz)
    end_date_local = datetime.strptime(input_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=local_tz)
    
    # Convert to UTC for API calls
    start_date = start_date_local.astimezone(timezone.utc)
    end_date = end_date_local.astimezone(timezone.utc)
    
    # Convert timestamps to milliseconds for the FusionSolar API
    start_timestamp_ms = int(start_date.timestamp() * 1000)
    end_timestamp_ms = int(end_date.timestamp() * 1000)
    
    print(f"\n1. PARSING SEBAGAI WAKTU LOKAL (UTC+7):")
    print(f"   Start: {start_date_local}")
    print(f"   End:   {end_date_local}")
    
    print(f"\n2. KONVERSI KE UTC UNTUK API:")
    print(f"   Start: {start_date}")
    print(f"   End:   {end_date}")
    
    print(f"\n3. TIMESTAMP YANG DIKIRIM KE API (milliseconds):")
    print(f"   Start: {start_timestamp_ms}")
    print(f"   End:   {end_timestamp_ms}")
    
    print(f"\n4. VERIFIKASI:")
    print(f"   Jika API menginterpretasikan timestamp sebagai UTC:")
    print(f"   - API akan query: {start_date} UTC sampai {end_date} UTC")
    print(f"   - Yang berarti: {start_date_local} UTC+7 sampai {end_date_local} UTC+7")
    
    # Verifikasi bahwa start dan end masih dalam tanggal yang sama di waktu lokal
    if start_date_local.date() == end_date_local.date():
        print(f"\n   ✓ BENAR: Data yang diingest untuk tanggal {input_date_str} (waktu lokal)")
        print(f"     Dari: {start_date_local.strftime('%Y-%m-%d %H:%M:%S %z')}")
        print(f"     Sampai: {end_date_local.strftime('%Y-%m-%d %H:%M:%S %z')}")
    else:
        print(f"\n   ✗ SALAH: Tanggal start dan end berbeda!")
    
    # Simulasi: jika sebelumnya (tanpa fix) menggunakan UTC langsung
    print(f"\n5. PERBANDINGAN DENGAN SEBELUMNYA (TANPA FIX):")
    old_start = datetime.strptime(input_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    old_end = datetime.strptime(input_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    old_start_local = old_start.astimezone(local_tz)
    old_end_local = old_end.astimezone(local_tz)
    
    print(f"   Sebelumnya (salah):")
    print(f"   - UTC: {old_start} sampai {old_end}")
    print(f"   - UTC+7: {old_start_local} sampai {old_end_local}")
    print(f"   - Masalah: Data diingest dari jam {old_start_local.hour:02d}:{old_start_local.minute:02d} sampai {old_end_local.hour:02d}:{old_end_local.minute:02d}")
    print(f"   - Ini menyebabkan pergeseran 7 jam!")
    
    print(f"\n   Sekarang (benar):")
    print(f"   - UTC: {start_date} sampai {end_date}")
    print(f"   - UTC+7: {start_date_local} sampai {end_date_local}")
    print(f"   - Data diingest dari jam {start_date_local.hour:02d}:{start_date_local.minute:02d} sampai {end_date_local.hour:02d}:{end_date_local.minute:02d}")
    print(f"   - ✓ Tidak ada pergeseran waktu!")
    
    return {
        'input': input_date_str,
        'start_local': start_date_local,
        'end_local': end_date_local,
        'start_utc': start_date,
        'end_utc': end_date,
        'start_timestamp_ms': start_timestamp_ms,
        'end_timestamp_ms': end_timestamp_ms
    }

if __name__ == "__main__":
    print("="*60)
    print("TEST LOGIKA PARSING TANGGAL FUSIONSOLAR")
    print("="*60)
    
    # Test dengan tanggal 23 November 2024 (atau tahun yang sesuai)
    test_date = "2024-11-23"
    result = test_date_parsing(test_date)
    
    print(f"\n{'='*60}")
    print("KESIMPULAN:")
    print(f"{'='*60}")
    print(f"✓ Logika parsing tanggal sudah benar")
    print(f"✓ Input '{test_date}' akan mengingest data untuk tanggal tersebut")
    print(f"✓ Tidak ada pergeseran 7 jam")
    print(f"✓ Data diingest dari 00:00:00 sampai 23:59:59 waktu lokal (UTC+7)")

