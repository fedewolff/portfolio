WITH
  last_user_locations AS (
    SELECT user_id, MAX(location_id) AS location_id
    FROM `project.dataset.user_locations`
    WHERE user_location_type_id = 1
    GROUP BY user_id
  ),

  user_registered AS (
    SELECT
      u.id AS user_id,
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
    WHERE role_id IN (1, 3)
      AND u.deleted_at IS NULL
      AND is_test IS NOT TRUE
  ),

  date_sequence AS (
    SELECT
      user_id, instagram_tier, cities, registration_date,
      DATE_ADD(registration_date, INTERVAL day_number DAY) AS current_date,
      day_number AS days_since_registered
    FROM user_registered,
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), registration_date, DAY))) AS day_number
  ),

  app_launched_events AS (
    SELECT
      SAFE_CAST(user_id AS NUMERIC) AS user_id,
      DATE(created_at_timestamp) AS event_date,
      COUNTIF(event_name = 'Session Concluded') AS sessions_per_day,
      SUM(CAST(event_prop_session_length AS NUMERIC)) AS sum_sessions_length_per_day
    FROM `project.dataset.clevertap_events`
    LEFT JOIN `project.dataset.users` u ON SAFE_CAST(user_id AS NUMERIC) = u.id
    WHERE event_name IN (
      'Session Concluded', 'App Launched', 'HOME_TAB_SCREEN_VIEW', 'JOB_LIST_SCREEN_VIEW',
      'INFLUUR_PREMIUM_BUTTON_CLICK', 'NAVIGATION_BAR_LWT_BUTTON_CLICK',
      'PROFILE_TAB_SCREEN_VIEW', 'WALLET_SCREEN_VIEW', 'LWTS_SCREEN_TAB_ITEM')
      AND role_id IN (1, 3)
      AND deleted_at IS NULL
      AND is_test IS NOT TRUE
    GROUP BY 1, 2
  ),

  retention AS (
    SELECT
      ds.registration_date, instagram_tier, cities, ds.days_since_registered,
      COUNT(DISTINCT ale.user_id) AS retained_users,
      SUM(COALESCE(ale.sessions_per_day, 0)) AS sessions_per_date,
      SUM(COALESCE(ale.sum_sessions_length_per_day, 0)) AS total_time_per_date
    FROM date_sequence ds
    LEFT JOIN app_launched_events ale
      ON ds.user_id = ale.user_id AND ds.current_date = ale.event_date
    GROUP BY ds.registration_date, instagram_tier, ds.days_since_registered, cities
  ),

  eligible_users AS (
    SELECT
      registration_date, instagram_tier, cities, days_since_registered,
      COUNT(DISTINCT user_id) AS eligible_users
    FROM date_sequence
    GROUP BY registration_date, days_since_registered, instagram_tier, cities
  )

SELECT
  eu.registration_date,
  eu.days_since_registered,
  eu.instagram_tier AS eligible_instagram_tier,
  eu.cities AS eligible_cities,
  COALESCE(r.retained_users, 0) AS retained_users,
  eu.eligible_users,
  COALESCE(r.sessions_per_date, 0) AS sessions_number_of_retained_users,
  COALESCE(r.total_time_per_date, 0) AS daily_time_of_retained_users
FROM eligible_users eu
LEFT JOIN retention r
  ON eu.registration_date = r.registration_date
  AND eu.days_since_registered = r.days_since_registered
  AND eu.instagram_tier = r.instagram_tier
  AND eu.cities = r.cities;
