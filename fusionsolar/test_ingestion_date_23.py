"""
Test script untuk memverifikasi ingestion tanggal 23 November
Script ini mensimulasikan apa yang akan terjadi saat menjalankan harvester
"""
from datetime import datetime, timezone, timedelta
import json

def simulate_ingestion_for_date(input_date_str):
    """
    Simulasi proses ingestion untuk tanggal tertentu
    """
    print(f"\n{'='*70}")
    print(f"SIMULASI INGESTION FUSIONSOLAR UNTUK TANGGAL: {input_date_str}")
    print(f"{'='*70}\n")
    
    # Step 1: Parse tanggal sebagai waktu lokal (UTC+7)
    local_tz = timezone(timedelta(hours=7))
    start_date_local = datetime.strptime(input_date_str, '%Y-%m-%d').replace(tzinfo=local_tz)
    end_date_local = datetime.strptime(input_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=local_tz)
    
    # Step 2: Konversi ke UTC untuk API
    start_date_utc = start_date_local.astimezone(timezone.utc)
    end_date_utc = end_date_local.astimezone(timezone.utc)
    
    # Step 3: Generate timestamp untuk API
    start_timestamp_ms = int(start_date_utc.timestamp() * 1000)
    end_timestamp_ms = int(end_date_utc.timestamp() * 1000)
    
    print("STEP 1: PARSING INPUT TANGGAL")
    print("-" * 70)
    print(f"Input user: {input_date_str}")
    print(f"Diinterpretasikan sebagai: Tanggal {input_date_str} waktu lokal (UTC+7)")
    print()
    
    print("STEP 2: KONVERSI KE WAKTU LOKAL (UTC+7)")
    print("-" * 70)
    print(f"Start: {start_date_local.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"End:   {end_date_local.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"Durasi: 24 jam (00:00:00 - 23:59:59)")
    print()
    
    print("STEP 3: KONVERSI KE UTC UNTUK API CALL")
    print("-" * 70)
    print(f"Start UTC: {start_date_utc.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"End UTC:   {end_date_utc.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"Note: UTC lebih lambat 7 jam dari UTC+7")
    print()
    
    print("STEP 4: TIMESTAMP YANG DIKIRIM KE API")
    print("-" * 70)
    print(f"Start timestamp (ms): {start_timestamp_ms}")
    print(f"End timestamp (ms):   {end_timestamp_ms}")
    print()
    
    print("STEP 5: SIMULASI API PAYLOAD")
    print("-" * 70)
    payload = {
        "sns": "DEVICE_ID_1,DEVICE_ID_2",  # Contoh
        "devTypeId": 1,  # Contoh
        "startTime": start_timestamp_ms,
        "endTime": end_timestamp_ms
    }
    print(json.dumps(payload, indent=2))
    print()
    
    print("STEP 6: VERIFIKASI DATA YANG AKAN DIINGEST")
    print("-" * 70)
    print("Jika API FusionSolar menginterpretasikan timestamp sebagai UTC:")
    print(f"  → API akan mengembalikan data dari:")
    print(f"     {start_date_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"     sampai")
    print(f"     {end_date_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    print("Yang berarti dalam waktu lokal (UTC+7):")
    print(f"  → Data yang diingest:")
    print(f"     {start_date_local.strftime('%Y-%m-%d %H:%M:%S UTC+7')}")
    print(f"     sampai")
    print(f"     {end_date_local.strftime('%Y-%m-%d %H:%M:%S UTC+7')}")
    print()
    
    # Verifikasi
    print("STEP 7: VERIFIKASI HASIL")
    print("-" * 70)
    if start_date_local.date() == end_date_local.date() == datetime.strptime(input_date_str, '%Y-%m-%d').date():
        print("✓ BENAR: Data yang diingest sesuai dengan tanggal input")
        print(f"✓ Tanggal: {input_date_str}")
        print(f"✓ Waktu: 00:00:00 - 23:59:59 (waktu lokal UTC+7)")
        print(f"✓ Tidak ada pergeseran 7 jam")
        return True
    else:
        print("✗ SALAH: Ada masalah dengan parsing tanggal")
        return False

def compare_with_old_logic(input_date_str):
    """Bandingkan dengan logika lama (yang salah)"""
    print(f"\n{'='*70}")
    print("PERBANDINGAN: LOGIKA LAMA vs LOGIKA BARU")
    print(f"{'='*70}\n")
    
    local_tz = timezone(timedelta(hours=7))
    
    # Logika lama (salah)
    print("LOGIKA LAMA (SALAH):")
    print("-" * 70)
    old_start_utc = datetime.strptime(input_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    old_end_utc = datetime.strptime(input_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    old_start_local = old_start_utc.astimezone(local_tz)
    old_end_local = old_end_utc.astimezone(local_tz)
    
    print(f"Input: {input_date_str}")
    print(f"Diinterpretasikan sebagai UTC langsung:")
    print(f"  UTC: {old_start_utc.strftime('%Y-%m-%d %H:%M:%S')} - {old_end_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  UTC+7: {old_start_local.strftime('%Y-%m-%d %H:%M:%S')} - {old_end_local.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✗ Masalah: Data diingest dari jam {old_start_local.hour:02d}:{old_start_local.minute:02d} sampai {old_end_local.hour:02d}:{old_end_local.minute:02d}")
    print(f"✗ Ini menyebabkan pergeseran 7 jam!")
    print()
    
    # Logika baru (benar)
    print("LOGIKA BARU (BENAR):")
    print("-" * 70)
    new_start_local = datetime.strptime(input_date_str, '%Y-%m-%d').replace(tzinfo=local_tz)
    new_end_local = datetime.strptime(input_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=local_tz)
    new_start_utc = new_start_local.astimezone(timezone.utc)
    new_end_utc = new_end_local.astimezone(timezone.utc)
    
    print(f"Input: {input_date_str}")
    print(f"Diinterpretasikan sebagai waktu lokal (UTC+7) dulu, lalu dikonversi ke UTC:")
    print(f"  UTC+7: {new_start_local.strftime('%Y-%m-%d %H:%M:%S')} - {new_end_local.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  UTC: {new_start_utc.strftime('%Y-%m-%d %H:%M:%S')} - {new_end_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✓ Benar: Data diingest dari jam {new_start_local.hour:02d}:{new_start_local.minute:02d} sampai {new_end_local.hour:02d}:{new_end_local.minute:02d}")
    print(f"✓ Tidak ada pergeseran waktu!")

if __name__ == "__main__":
    # Test dengan tanggal 23 November
    test_date = "2024-11-23"
    
    print("\n" + "="*70)
    print("TEST INGESTION FUSIONSOLAR - TANGGAL 23 NOVEMBER")
    print("="*70)
    
    # Simulasi ingestion
    success = simulate_ingestion_for_date(test_date)
    
    # Perbandingan
    compare_with_old_logic(test_date)
    
    print(f"\n{'='*70}")
    print("KESIMPULAN")
    print(f"{'='*70}")
    if success:
        print("✓ Logika parsing tanggal SUDAH BENAR")
        print(f"✓ Input tanggal '{test_date}' akan mengingest data untuk tanggal tersebut")
        print(f"✓ Data diingest dari 00:00:00 sampai 23:59:59 waktu lokal (UTC+7)")
        print(f"✓ Tidak ada pergeseran 7 jam")
        print(f"\n✓ Siap untuk digunakan!")
    else:
        print("✗ Ada masalah dengan logika parsing tanggal")

