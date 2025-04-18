from collections import defaultdict
from sqlmodel import text
from apps.database import get_session

query = text(
    """
    SELECT
    TO_CHAR (DATE_TRUNC ('month', st.date), 'YYYY-MM') AS month,
    s.name AS source_name,
    SUM(st.revenue) AS total_revenue
FROM
    public.source_traffic st
    JOIN public.source s ON st.source_id = s.id
    JOIN public.hotel h ON st.hotel_id = h.id
WHERE
    h.is_active = true
    AND st.date >= DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '6 month'
    AND st.date < DATE_TRUNC ('month', CURRENT_DATE)
GROUP BY
    month,
    s.name
ORDER BY
    month,
    s.name;
    """
)


def get_last_6_months_source_revenue():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


source_revenue_data = get_last_6_months_source_revenue()

if source_revenue_data:
    print("Last 6 Months Revenue by Source (with Average Revenue):\n")

    # Calculate total revenue and count per source
    total_per_source = defaultdict(float)
    count_per_source = defaultdict(int)

    for row in source_revenue_data:
        total_per_source[row.source_name] += row.total_revenue
        count_per_source[row.source_name] += 1

    # Calculate average revenue per source
    avg_per_source = {
        source: total / count_per_source[source]
        for source, total in total_per_source.items()
    }

    for row in source_revenue_data:
        avg_rev = avg_per_source[row.source_name]
        print(
            f"Month: {row.month}, Source: {row.source_name}, Revenue: {row.total_revenue:.2f}, Avg Revenue: {avg_rev:.2f}"
        )

else:
    print("No source revenue data found for the last 6 months.")
