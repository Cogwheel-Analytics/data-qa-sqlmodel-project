from collections import defaultdict
from sqlmodel import text
from apps.database import get_session

query = text(
    """
    WITH
    months AS (
        SELECT
            TO_CHAR (
                DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' * i,
                'YYYY-MM'
            ) AS month,
            DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' * i AS month_start
        FROM
            generate_series(1, 6) AS i
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
            DATE_TRUNC('month', cm.date) AS month_start,
            cm.channel_type_id,
            SUM(cm.revenue) AS total_revenue
        FROM
            public.channel_mix cm
            JOIN public.hotel h ON cm.hotel_id = h.id
        WHERE
            h.is_active = true
            AND cm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 month'
            AND cm.date < DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY
            DATE_TRUNC('month', cm.date),
            cm.channel_type_id
    ),
    combined AS (
        SELECT
            m.month_start,
            TO_CHAR(m.month_start, 'YYYY-MM') AS month,
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
    total_revenue
FROM combined
ORDER BY month_start ASC, channel_type_name;
    """
)


def get_monthly_channel_type_revenue():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


monthly_channel_revenue = get_monthly_channel_type_revenue()

if monthly_channel_revenue:
    print("Total Revenue by Month & Channel Type (Avg, Diff & Ratio):\n")

    # Collect total revenue per channel_type
    revenue_totals = defaultdict(float)
    revenue_counts = defaultdict(int)

    for row in monthly_channel_revenue:
        revenue_totals[row.channel_type_name] += row.total_revenue
        revenue_counts[row.channel_type_name] += 1

    # Calculate average revenue per channel_type
    average_revenue = {
        channel: revenue_totals[channel] / revenue_counts[channel]
        for channel in revenue_totals
    }

    for row in monthly_channel_revenue:
        avg = average_revenue[row.channel_type_name]
        diff = round(row.total_revenue - avg, 2)
        ratio = round(row.total_revenue / avg, 2) if avg != 0 else 0.0
        print(
            f"{row.month} - {row.channel_type_name}: "
            f"Total Revenue: {row.total_revenue:.2f}, "
            f"Avg: {avg:.2f}, "
            f"Diff: {diff:.2f}, "
            f"Ratio: {ratio:.2f}"
        )

else:
    print("No revenue data found.")
