from collections import defaultdict
from sqlmodel import text
from apps.database import get_session


def get_missing_visits_and_revenue_for_months(month_list):
    # Convert "YYYY-MM" to "YYYY-MM-01" for SQL-compatible date format
    month_dates = [f"{m}-01" for m in month_list]

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
        visit_revenue_aggregated AS (
            SELECT
                hotel_id,
                DATE_TRUNC('month', date) AS month_start,
                COALESCE(SUM(traffic), 0) AS total_traffic,
                COALESCE(SUM(revenue), 0) AS total_revenue
            FROM public.visit_revenue
            WHERE date >= (SELECT MIN(month_start) FROM months)
              AND date < (SELECT MAX(month_start) + INTERVAL '1 month' FROM months)
            GROUP BY hotel_id, DATE_TRUNC('month', date)
        ),
        missing_data AS (
            SELECT hmc.hotel_code, TO_CHAR(hmc.month_start, 'YYYY-MM') AS month
            FROM hotel_month_combinations hmc
            LEFT JOIN visit_revenue_aggregated vra
              ON hmc.hotel_id = vra.hotel_id AND hmc.month_start = vra.month_start
            WHERE vra.hotel_id IS NULL
               OR (vra.total_traffic = 0 AND vra.total_revenue = 0)
        )
        SELECT * FROM missing_data
        ORDER BY hotel_code, month;
        """
    )

    with get_session() as session:
        result = session.execute(query, {"month_list": month_dates}).fetchall()
        return result


custom_months = ["2023-12", "2024-01", "2025-03"]
missing_rows = get_missing_visits_and_revenue_for_months(custom_months)

hotel_months = defaultdict(list)
for row in missing_rows:
    hotel_months[row.hotel_code].append(row.month)

if hotel_months:
    print("Hotels with Missing Visits & Revenue Data for Custom Months:\n")
    for hotel_code, months in hotel_months.items():
        print(f"Hotel Code: {hotel_code}, Missing Months: {months}")
else:
    print("All active hotels have visits and revenue data for the selected months.")
