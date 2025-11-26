-- ============================================================================
-- Subscription View Builder
-- ============================================================================
-- This query builds a comprehensive subscription view by combining all 
-- transaction types (ACT, RENO, DCT, CNR, RFND) and handling edge cases:
--   - Missing activation records (subscriptions that start with RENO)
--   - CPC upgrades (when a subscription changes service)
--   - Multiple CPCs per subscription (tracked as a list)
-- ============================================================================

CREATE OR REPLACE TABLE subscriptions AS
WITH 
-- Get all transactions (ACT + RENO) to handle missing activations and upgrades
all_transactions AS (
    SELECT 
        subscription_id,
        tmuserid,
        msisdn,
        cpc,
        trans_type_id,
        trans_date,
        act_date,
        reno_date,
        camp_name,
        channel_act as channel,
        rev,
        year_month,
        'ACT' as transaction_type
    FROM read_parquet('{parquet_path}/act/**/*.parquet', hive_partitioning=true)
    
    UNION ALL
    
    SELECT 
        subscription_id,
        tmuserid,
        msisdn,
        cpc,
        trans_type_id,
        trans_date,
        act_date,
        reno_date,
        camp_name,
        channel_act as channel,
        rev,
        year_month,
        'RENO' as transaction_type
    FROM read_parquet('{parquet_path}/reno/**/*.parquet', hive_partitioning=true)
),

-- Get list of all CPCs per subscription (ordered by first appearance)
cpc_with_order AS (
    SELECT 
        subscription_id,
        cpc,
        MIN(trans_date) as first_seen
    FROM all_transactions
    GROUP BY subscription_id, cpc
),

cpc_list AS (
    SELECT 
        subscription_id,
        LIST(cpc ORDER BY first_seen) as cpc_list,
        COUNT(DISTINCT cpc) as cpc_count
    FROM cpc_with_order
    GROUP BY subscription_id
),

-- Get first transaction per subscription (handles missing ACT records)
first_transaction AS (
    SELECT 
        subscription_id,
        tmuserid,
        msisdn,
        cpc as first_cpc,
        act_date as activation_date,
        trans_date as first_trans_date,
        camp_name as activation_campaign,
        channel as activation_channel,
        year_month as activation_month
    FROM all_transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date ASC) = 1
),

-- Get actual ACT records (to distinguish from inferred activations)
actual_activations AS (
    SELECT 
        subscription_id,
        trans_date as actual_act_trans_date,
        rev as activation_revenue,
        cpc as act_cpc
    FROM all_transactions
    WHERE transaction_type = 'ACT'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date ASC) = 1
),

-- Detect CPC upgrades (when CPC changes after activation)
cpc_changes AS (
    SELECT 
        subscription_id,
        cpc as new_cpc,
        trans_date as upgrade_date,
        rev as upgrade_revenue
    FROM all_transactions
    WHERE transaction_type = 'ACT' 
    AND trans_type_id = 1  -- Upgrade transaction
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date DESC) = 1
),

-- Get current CPC (last known CPC for the subscription)
current_cpc AS (
    SELECT 
        subscription_id,
        cpc as current_cpc
    FROM all_transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date DESC) = 1
),

-- Aggregate all renewals
renewals AS (
    SELECT 
        subscription_id,
        COUNT(*) as total_renewals,
        SUM(rev) as total_renewal_revenue,
        MAX(trans_date) as last_renewal_date,
        MIN(trans_date) as first_renewal_date
    FROM all_transactions
    WHERE transaction_type = 'RENO'
    GROUP BY subscription_id
),

-- Get deactivations (excluding UPGRADE deactivations)
deactivations AS (
    SELECT 
        subscription_id,
        trans_date as deactivation_date,
        channel_dct as deactivation_mode
    FROM read_parquet('{parquet_path}/dct/**/*.parquet', hive_partitioning=true)
    WHERE channel_dct != 'UPGRADE'  -- Exclude upgrade-related DCT
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY trans_date DESC) = 1
),

-- Get cancellations
cancellations AS (
    SELECT 
        sbn_id as subscription_id,
        cancel_date as cancellation_date,
        mode as cancellation_mode
    FROM read_parquet('{parquet_path}/cnr/**/*.parquet', hive_partitioning=true)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sbn_id ORDER BY cancel_date DESC) = 1
),

-- Get refunds
refunds AS (
    SELECT 
        sbnid as subscription_id,
        COUNT(*) as refund_count,
        SUM(rfnd_amount) as total_refunded,
        MAX(refnd_date) as last_refund_date
    FROM read_parquet('{parquet_path}/rfnd/**/*.parquet', hive_partitioning=true)
    GROUP BY sbnid
)

-- Final aggregation
SELECT 
    ft.subscription_id,
    ft.tmuserid,
    ft.msisdn,
    
    -- CPC tracking (as list)
    cpc_l.cpc_list,
    cpc_l.cpc_count,
    ft.first_cpc,
    cc.current_cpc,
    CASE WHEN cpc_l.cpc_count > 1 THEN TRUE ELSE FALSE END as has_upgraded,
    cpc_ch.upgrade_date,
    cpc_ch.new_cpc as upgraded_to_cpc,
    
    -- Activation info
    ft.activation_date,
    COALESCE(aa.actual_act_trans_date, ft.first_trans_date) as activation_trans_date,
    CASE WHEN aa.subscription_id IS NULL THEN TRUE ELSE FALSE END as missing_act_record,
    ft.activation_campaign,
    ft.activation_channel,
    COALESCE(aa.activation_revenue, 0) as activation_revenue,
    ft.activation_month,
    
    -- Renewal info
    COALESCE(r.total_renewals, 0) as renewal_count,
    COALESCE(r.total_renewal_revenue, 0) as renewal_revenue,
    r.last_renewal_date,
    r.first_renewal_date,
    COALESCE(r.last_renewal_date, ft.activation_date) as last_activity_date,
    
    -- Deactivation info
    d.deactivation_date,
    d.deactivation_mode,
    
    -- Cancellation info
    c.cancellation_date,
    c.cancellation_mode,
    
    -- Refund info
    COALESCE(rf.refund_count, 0) as refund_count,
    COALESCE(rf.total_refunded, 0) as total_refunded,
    rf.last_refund_date,
    
    -- Calculated fields
    COALESCE(aa.activation_revenue, 0) + COALESCE(r.total_renewal_revenue, 0) as total_revenue,
    COALESCE(aa.activation_revenue, 0) + COALESCE(r.total_renewal_revenue, 0) + COALESCE(cpc_ch.upgrade_revenue, 0) as total_revenue_with_upgrade,
    
    -- Status determination
    CASE 
        WHEN c.cancellation_date IS NOT NULL THEN 'Cancelled'
        WHEN d.deactivation_date IS NOT NULL THEN 'Deactivated'
        ELSE 'Active'
    END as subscription_status,
    
    -- Lifetime calculation (days)
    CASE 
        WHEN c.cancellation_date IS NOT NULL THEN 
            DATE_DIFF('day', ft.activation_date, c.cancellation_date)
        WHEN d.deactivation_date IS NOT NULL THEN 
            DATE_DIFF('day', ft.activation_date, d.deactivation_date)
        ELSE 
            DATE_DIFF('day', ft.activation_date, CURRENT_DATE)
    END as lifetime_days,
    
    -- End date (for easier filtering)
    COALESCE(c.cancellation_date, d.deactivation_date) as end_date
    
FROM first_transaction ft
LEFT JOIN cpc_list cpc_l ON ft.subscription_id = cpc_l.subscription_id
LEFT JOIN actual_activations aa ON ft.subscription_id = aa.subscription_id
LEFT JOIN current_cpc cc ON ft.subscription_id = cc.subscription_id
LEFT JOIN cpc_changes cpc_ch ON ft.subscription_id = cpc_ch.subscription_id
LEFT JOIN renewals r ON ft.subscription_id = r.subscription_id
LEFT JOIN deactivations d ON ft.subscription_id = d.subscription_id
LEFT JOIN cancellations c ON ft.subscription_id = c.subscription_id
LEFT JOIN refunds rf ON ft.subscription_id = rf.subscription_id
