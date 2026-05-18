-- ============================================
-- Update Cleaning Sequence Script
-- ============================================
-- Script untuk menghitung dan update cleaning_sequence
-- berdasarkan urutan cleaning_date per site
-- ============================================

-- Method 1: Update langsung dengan query
UPDATE staging.seed_cleaning_log cl
SET cleaning_sequence = sub.sequence_num,
    updated_at = NOW()
FROM (
    SELECT 
        id,
        ROW_NUMBER() OVER (
            PARTITION BY asset_type, COALESCE(site_id, ''), COALESCE(site_name, '')
            ORDER BY cleaning_date ASC
        ) as sequence_num
    FROM staging.seed_cleaning_log
    WHERE is_active = TRUE
) sub
WHERE cl.id = sub.id;

-- Method 2: Gunakan function (jika sudah dibuat)
-- SELECT staging.update_cleaning_sequence();

-- Verifikasi hasil
SELECT 
    asset_type,
    COALESCE(site_id, site_name) as site_identifier,
    cleaning_date,
    cleaning_sequence,
    CASE 
        WHEN cleaning_sequence = 1 THEN 'Pertama'
        WHEN cleaning_sequence = 2 THEN 'Kedua'
        WHEN cleaning_sequence = 3 THEN 'Ketiga'
        ELSE 'Ke-' || cleaning_sequence::text
    END as cleaning_ke,
    notes
FROM staging.seed_cleaning_log
WHERE is_active = TRUE
ORDER BY asset_type, COALESCE(site_id, site_name), cleaning_date ASC;

