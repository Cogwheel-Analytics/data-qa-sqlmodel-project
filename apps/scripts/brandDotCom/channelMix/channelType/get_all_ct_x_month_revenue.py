from sqlmodel import text
from apps.database import get_session

query = text(
    """
    WITH
    months AS (
        SELECT
            TO_CHAR (
                DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '1 month' * i,
                'YYYY-MM'
            ) AS month,
            DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '1 month' * i AS month_start
        FROM
            generate_series (1, 6) AS i -- start from 1 to skip current month
    ),
    channel_types AS (
        SELECT
            id AS channel_type_id,
            name AS channel_type_name
        FROM
            public.channel_type
    ),
    revenue_data AS (
        SELECT
            DATE_TRUNC ('month', cm.date) AS month_start,
            cm.channel_type_id,
            SUM(cm.revenue) AS total_revenue
        FROM
            public.channel_mix cm
            JOIN public.hotel h ON cm.hotel_id = h.id
        WHERE
            h.is_active = true
            AND cm.date >= DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '7 month'
            AND cm.date < DATE_TRUNC ('month', CURRENT_DATE)
        GROUP BY
            DATE_TRUNC ('month', cm.date),
            cm.channel_type_id
    ),
    combined AS (
        SELECT
            m.month_start,
            TO_CHAR(m.month_start, 'YYYY-MM') AS month,
            ct.channel_type_id,
            ct.channel_type_name,
            COALESCE(rd.total_revenue, 0) AS total_revenue
        FROM months m
        CROSS JOIN channel_types ct
        LEFT JOIN revenue_data rd
            ON rd.month_start = m.month_start
            AND rd.channel_type_id = ct.channel_type_id
    )
SELECT
    month,
    channel_type_name,
    total_revenue,
    AVG(total_revenue) OVER (PARTITION BY channel_type_name) AS average_revenue
FROM combined
ORDER BY month_start ASC, channel_type_name;
    """
)


def get_monthly_channel_type_revenue_last_x_months():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


monthly_channel_revenue = get_monthly_channel_type_revenue_last_x_months()

if monthly_channel_revenue:
    print("Revenue by Month & Channel Type (Last X Months):\n")
    for row in monthly_channel_revenue:
        print(
            f"{row.month} - {row.channel_type_name}: Total Revenue: {row.total_revenue}, Average Revenue: {row.average_revenue}"
        )
else:
    print("No revenue data found.")
