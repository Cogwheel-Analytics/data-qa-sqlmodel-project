from collections import defaultdict
from sqlmodel import text
from apps.database import get_session
from apps.utils.csv_export import export_hotel_months_to_csv


def get_missing_source_traffic_for_custom_months(month_list):
    # Convert months to first-day-of-month format: "YYYY-MM-01"
    formatted_months = [f"{m}-01" for m in month_list]

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
        source_traffic_aggregated AS (
            SELECT
                hotel_id,
                DATE_TRUNC('month', date) AS month_start,
                COALESCE(SUM(visits), 0) AS total_visits,
                COALESCE(SUM(revenue), 0) AS total_revenue
            FROM public.source_traffic
            WHERE DATE_TRUNC('month', date) IN (
                SELECT month_start FROM months
            )
            GROUP BY hotel_id, DATE_TRUNC('month', date)
        ),
        missing_data AS (
            SELECT hmc.hotel_code, TO_CHAR(hmc.month_start, 'YYYY-MM') AS month
            FROM hotel_month_combinations hmc
            LEFT JOIN source_traffic_aggregated sta
              ON hmc.hotel_id = sta.hotel_id AND hmc.month_start = sta.month_start
            WHERE sta.hotel_id IS NULL
               OR (sta.total_visits = 0 AND sta.total_revenue = 0)
        )
        SELECT * FROM missing_data
        ORDER BY hotel_code, month;
        """
    )

    with get_session() as session:
        result = session.execute(query, {"month_list": formatted_months}).fetchall()
        return result


custom_months = ["2023-12", "2024-01", "2024-03"]
missing_rows = get_missing_source_traffic_for_custom_months(custom_months)

hotel_months = defaultdict(list)
for row in missing_rows:
    hotel_months[row.hotel_code].append(row.month)


if hotel_months:
    print("Hotels with Missing SourceTraffic Data for Custom Months:\n")
    for hotel_code, months in hotel_months.items():
        print(f"Hotel Code: {hotel_code}, Missing Months: {months}")
else:
    print("All active hotels have source traffic data for the selected months.")


export_hotel_months_to_csv(
    hotel_months,
    "missing_source_traffic_custom_summary.csv",
    folder="csv_exports/sourceTraffic/customMonths",
)
