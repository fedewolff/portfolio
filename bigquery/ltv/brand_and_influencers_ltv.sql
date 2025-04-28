WITH
  last_user_locations AS (
    SELECT user_id, MAX(location_id) AS location_id
    FROM `project.dataset.user_locations`
    WHERE user_location_type_id = 1
    GROUP BY user_id
  ),

  user_info AS (
    SELECT
      u.id AS user_id,
      CASE WHEN u.role_id = 2 THEN 'brand' WHEN u.role_id IN (1, 3) THEN 'creator' END AS role_type,
      CASE
        WHEN instagram.followers BETWEEN 0 AND 9999 THEN 'Nano'
        WHEN instagram.followers BETWEEN 10000 AND 99999 THEN 'Micro'
        WHEN instagram.followers BETWEEN 100000 AND 499999 THEN 'Mid'
        WHEN instagram.followers BETWEEN 500000 AND 999999 THEN 'Macro'
        WHEN instagram.followers >= 1000000 THEN 'Mega'
        ELSE 'No Social Media verified'
      END AS instagram_tier,
      CASE
        WHEN city IN ('Miami', 'Miami Beach', 'Doral', 'Hialeah', 'Homestead', 'CoralGables') THEN 'Miami-Dade County'
        ELSE 'Other City'
      END AS cities,
      DATE(u.created_at) AS registration_date
    FROM `project.dataset.users` u
    LEFT JOIN `project.dataset.instagram_user_insights` instagram ON instagram.id = u.instagram_user_insights_id
    LEFT JOIN last_user_locations lul ON lul.user_id = u.id
    LEFT JOIN `project.dataset.locations` loc ON lul.location_id = loc.id
    WHERE role_id IN (1, 2, 3)
      AND u.deleted_at IS NULL
      AND is_test IS NOT TRUE
  ),

  transactions AS (
    SELECT DATE(t.created_at) AS transaction_date, t.from_user_id AS user_id, 'brand' AS role_type, t.service_fee AS revenue
    FROM `project.dataset.transactions` t
    LEFT JOIN `project.dataset.users` u ON u.id = t.from_user_id
    LEFT JOIN `project.dataset.users` b ON b.id = t.to_user_id
    WHERE u.role_id = 2
      AND u.deleted_at IS NULL
      AND u.is_test IS FALSE
      AND b.deleted_at IS NULL
      AND b.is_test IS FALSE
      AND LEFT(t.transaction_id, 2) IN ('ch', 'py', 'pi')
      AND t.service_fee > 0

    UNION ALL

    SELECT DATE(t.created_at) AS transaction_date, t.to_user_id AS user_id, 'creator' AS role_type, t.creator_service_fee AS revenue
    FROM `project.dataset.transactions` t
    LEFT JOIN `project.dataset.users` u ON u.id = t.to_user_id
    LEFT JOIN `project.dataset.users` b ON b.id = t.from_user_id
    WHERE u.role_id IN (1, 3)
      AND u.deleted_at IS NULL
      AND u.is_test IS FALSE
      AND b.deleted_at IS NULL
      AND b.is_test IS FALSE
      AND LEFT(t.transaction_id, 2) = 'tr'
      AND t.creator_service_fee > 0
  ),

  first_transaction AS (
    SELECT user_id, MIN(transaction_date) AS first_transaction_date
    FROM transactions
    GROUP BY user_id
  ),

  users_with_transactions AS (
    SELECT ui.*, ft.first_transaction_date
    FROM user_info ui
    INNER JOIN first_transaction ft ON ui.user_id = ft.user_id
  ),

  weekly_intervals AS (
    SELECT
      user_id, role_type, instagram_tier, cities, registration_date, first_transaction_date, week_number,
      DATE_ADD(first_transaction_date, INTERVAL (week_number * 7) DAY) AS period_start_date,
      DATE_ADD(first_transaction_date, INTERVAL ((week_number + 1) * 7) DAY) AS period_end_date,
      DATE_TRUNC(DATE_ADD(first_transaction_date, INTERVAL (week_number * 7) DAY), WEEK(SUNDAY)) AS transaction_week
    FROM users_with_transactions,
    UNNEST(GENERATE_ARRAY(0, 52)) AS week_number
    WHERE DATE_ADD(first_transaction_date, INTERVAL ((week_number + 1) * 7) DAY) <= CURRENT_DATE()
  ),

  user_transactions_by_week AS (
    SELECT
      wi.*, SUM(COALESCE(t.revenue, 0)) AS weekly_revenue,
      COUNT(DISTINCT t.transaction_date) AS transaction_count
    FROM weekly_intervals wi
    LEFT JOIN transactions t
      ON wi.user_id = t.user_id
      AND t.transaction_date >= wi.period_start_date
      AND t.transaction_date < wi.period_end_date
    GROUP BY wi.user_id, wi.role_type, wi.instagram_tier, wi.cities, wi.registration_date,
             wi.first_transaction_date, wi.week_number, wi.transaction_week
  ),

  user_cumulative_ltv AS (
    SELECT
      user_id, role_type, instagram_tier, cities, registration_date, first_transaction_date, week_number, transaction_week, weekly_revenue,
      SUM(weekly_revenue) OVER (PARTITION BY user_id ORDER BY week_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_ltv,
      SUM(transaction_count) OVER (PARTITION BY user_id ORDER BY week_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_transaction_count
    FROM user_transactions_by_week
  ),

  users_with_second_transaction AS (
    SELECT
      role_type, instagram_tier, cities, DATE_TRUNC(first_transaction_date, WEEK) AS first_transaction_week,
      week_number, transaction_week,
      COUNT(DISTINCT user_id) AS users_with_second_transaction
    FROM user_cumulative_ltv
    WHERE cumulative_transaction_count > 1
    GROUP BY role_type, instagram_tier, cities, first_transaction_week, week_number, transaction_week
  ),

  aggregated_ltv AS (
    SELECT
      role_type, instagram_tier, cities, DATE_TRUNC(first_transaction_date, WEEK) AS first_transaction_week,
      week_number, transaction_week,
      COUNT(DISTINCT user_id) AS total_users,
      SUM(cumulative_ltv) AS total_cumulative_ltv,
      AVG(cumulative_ltv) AS average_cumulative_ltv,
      SUM(weekly_revenue) AS cohort_weekly_revenue
    FROM user_cumulative_ltv
    GROUP BY role_type, instagram_tier, cities, first_transaction_week, week_number, transaction_week
  )

SELECT
  a.role_type, a.instagram_tier, a.cities, a.first_transaction_week, a.week_number, a.transaction_week,
  a.total_users AS eligible_users,
  COALESCE(s.users_with_second_transaction, 0) AS users_with_second_transaction,
  a.total_cumulative_ltv,
  a.average_cumulative_ltv,
  CASE WHEN a.total_users > 0 THEN a.total_cumulative_ltv / a.total_users ELSE 0 END AS ltv_per_user,
  CASE WHEN a.total_users > 0 THEN COALESCE(s.users_with_second_transaction, 0) / a.total_users ELSE 0 END AS second_transaction_rate,
  a.cohort_weekly_revenue
FROM aggregated_ltv a
LEFT JOIN users_with_second_transaction s
  ON a.role_type = s.role_type
  AND a.instagram_tier = s.instagram_tier
  AND a.cities = s.cities
  AND a.first_transaction_week = s.first_transaction_week
  AND a.week_number = s.week_number
  AND a.transaction_week = s.transaction_week
ORDER BY a.first_transaction_week DESC, a.role_type, a.instagram_tier, a.cities, a.week_number;

✅ This is clean, anonymized, and portfolio-ready.
