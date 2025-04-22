from collections import defaultdict
from sqlmodel import text
from apps.database import get_session


query = text(
    """
    WITH months AS (
        SELECT TO_DATE(m, 'YYYY-MM-DD') AS month_start
        FROM UNNEST(:month_list) AS m
    ),
    active_hotels AS (
        SELECT id AS hotel_id, code AS hotel_code
        FROM public.hotel
        WHERE is_active = true
    ),
    hotel_month_combinations AS (
        SELECT h.hotel_id, h.hotel_code, m.month_start
        FROM active_hotels h
        CROSS JOIN months m
    ),
    top_ref_aggregated AS (
        SELECT
            hotel_id,
            DATE_TRUNC('month', date) AS month_start,
            COALESCE(SUM(visits), 0) AS total_visits,
            COALESCE(SUM(booking), 0) AS total_bookings,
            COALESCE(SUM(room_nights), 0) AS total_room_nights,
            COALESCE(SUM(revenue), 0) AS total_revenue
        FROM public.top_ref_domain
        WHERE date >= (SELECT MIN(month_start) FROM months)
          AND date < (SELECT MAX(month_start) + INTERVAL '1 month' FROM months)
        GROUP BY hotel_id, DATE_TRUNC('month', date)
    ),
    missing_data AS (
        SELECT hmc.hotel_code, TO_CHAR(hmc.month_start, 'YYYY-MM') AS month
        FROM hotel_month_combinations hmc
        LEFT JOIN top_ref_aggregated tra
          ON hmc.hotel_id = tra.hotel_id AND hmc.month_start = tra.month_start
        WHERE tra.hotel_id IS NULL
           OR (
               tra.total_visits = 0 AND
               tra.total_bookings = 0 AND
               tra.total_room_nights = 0 AND
               tra.total_revenue = 0
           )
    )
    SELECT *
    FROM missing_data
    ORDER BY hotel_code, month;
    """
)


def get_custom_top_ref_missing_data(custom_months):
    month_dates = [f"{m}-01" for m in custom_months]  # convert to YYYY-MM-DD for SQL
    with get_session() as session:
        result = session.execute(query, {"month_list": month_dates}).fetchall()
        return result


custom_months = ["2023-12", "2024-01", "2025-03"]
missing_rows = get_custom_top_ref_missing_data(custom_months)

hotel_months = defaultdict(list)
for row in missing_rows:
    hotel_months[row.hotel_code].append(row.month)


if hotel_months:
    print("Hotels with Missing TopRefDomain Data for Selected Months:\n")
    for hotel_code, months in hotel_months.items():
        print(f"Hotel Code: {hotel_code}, Missing Months: {months}")
else:
    print("All active hotels have TopRefDomain data for the selected months.")
