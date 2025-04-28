WITH
  lwt_events AS (
    SELECT
      lwts.created_at AS lwt_date,
      event_prop_lwt_id,
      created_at_timestamp,
      event_name,
      CASE WHEN u.role_id = 1 THEN 'Influencer' WHEN u.role_id = 2 THEN 'Brand' ELSE 'Other' END AS role,
      CASE WHEN SAFE_CAST(ct.event_prop_creator_id AS NUMERIC) IS NULL THEN ct.user_id ELSE SAFE_CAST(ct.event_prop_creator_id AS NUMERIC) END AS creator_id,
      ROW_NUMBER() OVER (PARTITION BY event_prop_lwt_id ORDER BY created_at_timestamp) AS event_order
    FROM `project.dataset.clevertap_events` ct
    LEFT JOIN `project.dataset.lwts` lwts ON lwts.id = SAFE_CAST(ct.event_prop_lwt_id AS NUMERIC)
    LEFT JOIN `project.dataset.users` u ON u.id = ct.user_id
    WHERE event_name IN (
      'SEND_LWT_OFFER__SUCCESS', 'SEND_LWT_OFFER_ACCEPTED__SUCCESS',
      'ACCEPT_LWT_OFFER__SUCCESS', 'DECLINE_LWT_OFFER__SUCCESS',
      'SEND_LWT_NEGOTIATION__SUCCESS', 'SEND_LWT_APPLICATION__SUCCESS',
      'LWT_CHAT_SCREEN_TEXT_INPUT_SEND', 'SEND_LWT_DRAFT__SUCCESS',
      'POST_DELIVERED__SUCCESS', 'RECEIVE_PAID_LWT__SUCCESS'
    )
      AND created_at_timestamp >= TIMESTAMP('2024-01-01 00:00:00')
      AND u.is_test IS NOT TRUE
      AND u.deleted_at IS NULL
      AND lwts.deleted_at IS NULL
  ),

  lwt_with_events AS (
    SELECT
      l1.lwt_date, l1.event_prop_lwt_id, l1.event_name AS first_event, l1.role AS first_event_role,
      l1.creator_id,
      CASE WHEN country IN ('United States', 'USA') THEN 'USA' ELSE 'Other Countries' END AS countries,
      CASE WHEN city IN ('Miami', 'Miami Beach', 'Doral', 'Hialeah', 'Homestead', 'CoralGables') THEN 'Miami-Dade County' ELSE 'Other cities' END AS cities,
      l1.created_at_timestamp AS first_event_time,
      COALESCE(
        CASE
          WHEN l3.event_name = l1.event_name OR TIMESTAMP_DIFF(l3.created_at_timestamp, l1.created_at_timestamp, SECOND) <= 2 THEN 'No Following Event'
          WHEN l2.event_name = l1.event_name OR TIMESTAMP_DIFF(l2.created_at_timestamp, l1.created_at_timestamp, SECOND) <= 2 OR l2.role = 'Brand' THEN l3.event_name
          ELSE l2.event_name
        END,
        'No Following Event'
      ) AS second_event,
      COALESCE(
        CASE
          WHEN l3.event_name = l1.event_name OR TIMESTAMP_DIFF(l3.created_at_timestamp, l1.created_at_timestamp, SECOND) <= 2 THEN NULL
          WHEN l2.event_name = l1.event_name OR TIMESTAMP_DIFF(l2.created_at_timestamp, l1.created_at_timestamp, SECOND) <= 2 OR l2.role = 'Brand' THEN l3.role
          ELSE l2.role
        END,
        NULL
      ) AS second_event_role,
      COALESCE(
        CASE
          WHEN l3.event_name = l1.event_name OR TIMESTAMP_DIFF(l3.created_at_timestamp, l1.created_at_timestamp, SECOND) <= 2 THEN NULL
          WHEN l2.event_name = l1.event_name OR TIMESTAMP_DIFF(l2.created_at_timestamp, l1.created_at_timestamp, SECOND) <= 2 OR l2.role = 'Brand' THEN l3.created_at_timestamp
          ELSE l2.created_at_timestamp
        END,
        NULL
      ) AS second_event_time,
      COALESCE(
        TIMESTAMP_DIFF(
          CASE
            WHEN l3.event_name = l1.event_name OR TIMESTAMP_DIFF(l3.created_at_timestamp, l1.created_at_timestamp, SECOND) <= 2 THEN NULL
            WHEN l2.event_name = l1.event_name OR TIMESTAMP_DIFF(l2.created_at_timestamp, l1.created_at_timestamp, SECOND) <= 2 OR l2.role = 'Brand' THEN l3.created_at_timestamp
            ELSE l2.created_at_timestamp
          END,
          l1.created_at_timestamp,
          SECOND
        ),
        NULL
      ) AS time_diff_seconds,
      CASE WHEN COALESCE(l2.role, l3.role) = 'Influencer' THEN 'Influencer Response' ELSE 'No Influencer Response' END AS influencer_response_status,
      ROW_NUMBER() OVER (PARTITION BY l1.event_prop_lwt_id ORDER BY l1.created_at_timestamp ASC) AS rn
    FROM lwt_events l1
    LEFT JOIN lwt_events l2 ON l1.event_prop_lwt_id = l2.event_prop_lwt_id AND l2.event_order = l1.event_order + 1
    LEFT JOIN lwt_events l3 ON l1.event_prop_lwt_id = l3.event_prop_lwt_id AND l3.event_order = l1.event_order + 2
    LEFT JOIN (
      SELECT user_id, MAX(location_id) AS location_id
      FROM `project.dataset.user_locations`
      WHERE user_location_type_id = 1
      GROUP BY user_id
    ) last_user_location ON last_user_location.user_id = l1.creator_id
    LEFT JOIN `project.dataset.locations` loc ON loc.id = last_user_location.location_id
    WHERE l1.role = 'Brand'
  )

SELECT
  lwt_date,
  event_prop_lwt_id,
  creator_id,
  countries,
  cities,
  first_event,
  first_event_role,
  first_event_time,
  second_event,
  second_event_role,
  second_event_time,
  time_diff_seconds,
  CASE WHEN second_event_role IS NULL THEN 'No Influencer Response' ELSE 'Influencer answered' END AS influencer_response_status
FROM lwt_with_events
WHERE rn = 1
  AND (second_event_role = 'Influencer' OR second_event_role IS NULL);
