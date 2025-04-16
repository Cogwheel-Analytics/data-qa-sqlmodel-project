from sqlmodel import text
from apps.database import get_session

query = text(
    """
    SELECT
    DATE_TRUNC ('month', vnr.date) AS month,
    SUM(vnr.traffic) AS total_visits,
    SUM(vnr.revenue) AS total_revenue
FROM
    public.visit_revenue vnr
    JOIN public.hotel h ON vnr.hotel_id = h.id
WHERE
    h.is_active = true
    AND vnr.date >= DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '6 months'
    AND vnr.date < DATE_TRUNC ('month', CURRENT_DATE)
GROUP BY
    month
ORDER BY
    month;
    """
)


def get_last_x_months_visits_and_revenue():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


monthly_stats = get_last_x_months_visits_and_revenue()

if monthly_stats:
    print("Last 6 Months - Total Visits and Revenue (Active Hotels):\n")
    for row in monthly_stats:
        print(
            f"Month: {row.month.strftime('%Y-%m')}, Visits: {row.total_visits}, Revenue: {row.total_revenue}"
        )
else:
    print("No data found for active hotels in the last X months.")
