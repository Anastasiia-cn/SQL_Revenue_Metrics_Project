WITH filter_cte AS (
SELECT 
      user_id, 
      game_name, 
      DATE(date_trunc('month', payment_date)) AS payment_month,
      SUM(revenue_amount_usd) AS revenue_amount
FROM project.games_payments
GROUP BY user_id, game_name, DATE(DATE_TRUNC('month', payment_date))
),
months_cte AS (
SELECT 
     user_id, 
     game_name,
     payment_month,
     revenue_amount,
     LAG(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_month,
     LEAD(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS next_month,
     LAG(revenue_amount) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_month_revenue,
     DATE(payment_month - INTERVAL '1' MONTH) AS previous_calendar_month,
     date(payment_month + INTERVAL '1' MONTH) AS next_calendar_month
FROM filter_cte
),
revenue_metrics_cte AS (
SELECT
     user_id, 
     game_name,
     payment_month,
     revenue_amount,
     'mrr' AS revenue_type
FROM months_cte

UNION ALL

SELECT
     user_id, 
     game_name,
     payment_month,
     revenue_amount,
     'new_mrr' AS revenue_type
FROM months_cte
WHERE previous_month IS NULL

UNION ALL

SELECT
     user_id, 
     game_name,
     next_calendar_month,
     -revenue_amount AS revenue_amount,
     'churned_revenue' AS revenue_type
FROM months_cte
WHERE next_month IS NULL OR next_month != next_calendar_month

UNION ALL 

SELECT
     user_id, 
     game_name,
     payment_month,
     revenue_amount AS revenue_amount,
     'back_from_churn' AS revenue_type
FROM months_cte
WHERE previous_month IS NOT NULL AND previous_month != previous_calendar_month 

UNION ALL

SELECT
     user_id, 
     game_name,
     payment_month,
     (revenue_amount-previous_month_revenue) AS revenue_amount,
     'expansion_mrr' AS revenue_type
FROM months_cte
WHERE previous_month = previous_calendar_month AND revenue_amount > previous_month_revenue

UNION ALL

SELECT
     user_id, 
     game_name,
     payment_month,
     (revenue_amount-previous_month_revenue) AS revenue_amount,
     'contraction_mrr' AS revenue_type
FROM months_cte
WHERE previous_month = previous_calendar_month AND revenue_amount < previous_month_revenue
)

SELECT
     user_id, 
     revenue_metrics_cte.game_name,
     payment_month,
     revenue_amount,
     revenue_type,
     gpu.language,
     gpu.age,
     gpu.has_older_device_model
FROM revenue_metrics_cte
LEFT JOIN project.games_paid_users gpu USING(user_id)
