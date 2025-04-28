WITH
  latest_location AS (
    SELECT user_id, MAX(location_id) AS location_id
    FROM `project.dataset.user_locations`
    WHERE user_location_type_id = 1
    GROUP BY user_id
  ),

  tiktok_post_data AS (
    SELECT DISTINCT
      ttm.*, birthday,
      CASE
        WHEN LOWER(TRIM(gender)) IN ('female', 'trans femme', 'femboy', 'femenino') THEN 'Female'
        WHEN LOWER(TRIM(gender)) IN ('male', 'man', 'masculino') THEN 'Male'
        WHEN LOWER(TRIM(gender)) IN ('non binary', 'non-binary', 'no binarix', 'nonbinary', 'gender fluid', 'no binary', 'fluid', 'nb', 'no binario') THEN 'Non-binary'
        ELSE 'Other'
      END AS gender,
      u.tiktok_user_insights_id
    FROM `project.dataset.tiktok_media_insights_hist` ttm
    LEFT JOIN `project.dataset.users` u ON u.id = ttm.user_id
    WHERE TRUE
    ORDER BY ttm.created_at DESC
  ),

  summary AS (
    SELECT
      media_id, ttu.user_id, handle, gender, birthday,
      DATE(date_posted) AS date_posted,
      like_count, comment_count, play_count, share_count, save,
      followers,
      engagement_rate AS account_engagement_rate,
      DATE_DIFF(DATE(tiktok_post_data.created_at), DATE(date_posted), DAY) AS post_maturity_days,
      DATE(tiktok_post_data.created_at) AS date_post_data,
      DATE(ttu.created_at) AS date_followers_data
    FROM tiktok_post_data
    LEFT JOIN `project.dataset.tiktok_user_insights_hist` ttu ON ttu.id = tiktok_post_data.tiktok_user_insights_id
    WHERE TRUE
      AND DATE(tiktok_post_data.created_at) = DATE(ttu.created_at)
      AND DATE_DIFF(DATE(tiktok_post_data.created_at), DATE(date_posted), DAY) BETWEEN 7 AND 90
  )

SELECT
  summary.media_id,
  summary.user_id,
  handle,
  CASE
    WHEN location.city IN (
      'Miami', 'Miami Beach', 'Doral', 'Hialeah', 'Homestead', 'CoralGables') THEN 'Miami-Dade County'
    ELSE location.city
  END AS city,
  CASE
    WHEN location.country = 'United States' THEN 'USA'
    ELSE location.country
  END AS country,
  DATE(date_posted) AS date_posted,
  gender,
  birthday,
  flavour_id,
  DATE_DIFF(date_posted, birthday, YEAR) AS age_when_posted,
  CASE
    WHEN MAX(followers) >= 1000000 THEN 'Mega'
    WHEN MAX(followers) >= 500000 THEN 'Macro'
    WHEN MAX(followers) >= 100000 THEN 'Mid'
    WHEN MAX(followers) >= 10000 THEN 'Micro'
    WHEN MAX(followers) > 0 THEN 'Nano'
    ELSE 'No followers count information'
  END AS tiktok_tier,
  MAX(date_post_data) AS date_post_data,
  MAX(date_followers_data) AS date_followers_data,
  MAX(post_maturity_days) AS post_maturity_days,
  MAX(like_count) AS like_count,
  MAX(comment_count) AS comment_count,
  MAX(play_count) AS play_count,
  MAX(share_count) AS share_count,
  MAX(save) AS save,
  MAX(followers) AS followers,
  ROUND(((MAX(like_count) + MAX(comment_count) + MAX(share_count) + MAX(save)) /
    IF(MAX(followers) IS NULL OR MAX(followers) = 0, 1, MAX(followers))), 8) AS post_engagement_rate,
  MAX(account_engagement_rate) AS account_engagement_rate
FROM summary
LEFT JOIN `project.dataset.lwt_media` lm ON lm.media_id = summary.media_id
LEFT JOIN `project.dataset.lwts` l ON l.id = lm.lwt_id
LEFT JOIN `project.dataset.campaign_flavours` cf ON l.parent_job_id = cf.campaign_id
LEFT JOIN latest_location ON summary.user_id = latest_location.user_id
LEFT JOIN `project.dataset.locations` location ON location.id = latest_location.location_id
WHERE platform = 'tiktok'
  AND followers > 0;
