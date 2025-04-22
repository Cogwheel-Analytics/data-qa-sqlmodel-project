from sqlmodel import text
from apps.database import get_session


def get_monthly_total_revenue(start_date: str, end_date: str):
    query = text(
        """
        SELECT SUM(cm.revenue) AS monthly_total_revenue
        FROM public.channel_mix cm
        JOIN public.hotel h ON cm.hotel_id = h.id
        WHERE h.is_active = true
          AND cm.date BETWEEN :start_date AND :end_date
        """
    )

    with get_session() as session:
        result = session.execute(
            query, {"start_date": start_date, "end_date": end_date}
        ).fetchone()
        return result


start = "2024-03-01"
end = "2024-03-31"

total_revenue = get_monthly_total_revenue(start, end)

if total_revenue and total_revenue[0]:
    print(f"Total Revenue from {start} to {end} (Active Hotels): {total_revenue[0]}")
else:
    print(f"No revenue data found from {start} to {end} (Active Hotels).")
