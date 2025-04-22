from collections import defaultdict
from sqlmodel import text
from apps.database import get_session


def get_missing_sponsored_listing_for_custom_months(month_list):
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
        sponsored_listing_paid_media AS (
            SELECT
                hotel_id,
                DATE_TRUNC('month', date) AS month_start,
                COUNT(*) AS sponsored_listing_entries
            FROM public.paid_media pm
            JOIN public.media_channel mc ON pm.media_id = mc.id
            WHERE mc.name = 'SPONSORED LISTING'
              AND DATE_TRUNC('month', date) IN (
                  SELECT month_start FROM months
              )
            GROUP BY hotel_id, DATE_TRUNC('month', date)
        ),
        missing_data AS (
            SELECT hmc.hotel_code, TO_CHAR(hmc.month_start, 'YYYY-MM') AS month
            FROM hotel_month_combinations hmc
            LEFT JOIN sponsored_listing_paid_media ms
              ON hmc.hotel_id = ms.hotel_id AND hmc.month_start = ms.month_start
            WHERE ms.sponsored_listing_entries IS NULL
        )
        SELECT * FROM missing_data
        ORDER BY hotel_code, month;
        """
    )

    with get_session() as session:
        result = session.execute(query, {"month_list": formatted_months}).fetchall()
        return result


custom_months = ["2023-12", "2025-01", "2025-03"]
missing_rows = get_missing_sponsored_listing_for_custom_months(custom_months)

hotel_months = defaultdict(list)
for row in missing_rows:
    hotel_months[row.hotel_code].append(row.month)

if hotel_months:
    print("Hotels with Missing Meta Search (Paid Media) Data for Custom Months:\n")
    for hotel_code, months in hotel_months.items():
        print(f"Hotel Code: {hotel_code}, Missing Months: {months}")
else:
    print("All active hotels have sponsored listing data for the selected months.")
