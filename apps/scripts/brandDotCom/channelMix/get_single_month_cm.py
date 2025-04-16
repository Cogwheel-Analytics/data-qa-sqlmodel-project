from sqlmodel import text
from apps.database import get_session


def get_channel_mix_by_date_range(start_date: str, end_date: str):
    query = text(
        """
        SELECT cm.* 
        FROM public.channel_mix cm
        JOIN public.hotel h ON cm.hotel_id = h.id
        WHERE h.is_active = true
          AND cm.date BETWEEN :start_date AND :end_date
        ORDER BY cm.id ASC
        """
    )

    with get_session() as session:
        result = session.execute(
            query, {"start_date": start_date, "end_date": end_date}
        ).fetchall()
        return result


start_date = "2024-03-01"
end_date = "2024-03-31"

data = get_channel_mix_by_date_range(start_date, end_date)

if data:
    print(f"Channel Mix data from {start_date} to {end_date} (Active Hotels):\n")
    for row in data:
        print(row)
    print(f"\nTotal records: {len(data)}")
else:
    print(
        f"No data found in channel_mix table from {start_date} to {end_date} (Active Hotels)."
    )
