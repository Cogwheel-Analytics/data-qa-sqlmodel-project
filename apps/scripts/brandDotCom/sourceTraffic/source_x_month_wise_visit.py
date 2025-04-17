from sqlmodel import text
from apps.database import get_session


query = text(
    """
    SELECT
        TO_CHAR(DATE_TRUNC('month', st.date), 'YYYY-MM') AS month,
        s.name AS source_name,
        SUM(st.visits) AS total_visits
    FROM public.source_traffic st
    JOIN public.source s ON st.source_id = s.id
    JOIN public.hotel h ON st.hotel_id = h.id
    WHERE h.is_active = true
      AND st.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 month'
      AND st.date < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY month, s.name
    ORDER BY month, s.name;
    """
)


def get_last_6_months_source_visits():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


source_visits_data = get_last_6_months_source_visits()

if source_visits_data:
    print("Last 6 Months Visits by Source:\n")
    for row in source_visits_data:
        print(
            f"Month: {row.month}, Source: {row.source_name}, Visits: {row.total_visits}"
        )
else:
    print("No source visits data found for the last 6 months.")
