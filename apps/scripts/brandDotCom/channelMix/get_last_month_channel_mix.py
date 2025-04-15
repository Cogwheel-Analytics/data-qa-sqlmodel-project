from sqlmodel import text
from apps.database import get_session

# Raw SQL query to fetch ChannelMix data for last month (for all hotels)
query = text(
    """
    SELECT *
    FROM public.channel_mix
    WHERE date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 month'
      AND date < DATE_TRUNC('month', CURRENT_DATE)
    ORDER BY date;
"""
)


def get_last_month_channel_mix():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


channel_mix_data = get_last_month_channel_mix()

if channel_mix_data:
    print("Channel Mix data for last month:\n")
    for row in channel_mix_data:
        print(row)
    print(f"\nTotal records: {len(channel_mix_data)}")
else:
    print("No data found for last month.")
