from sqlmodel import text
from apps.database import get_session

# Raw SQL query to fetch all data from channel_mix table
query = text("SELECT * FROM channel_mix")


# Function to retrieve all data from channel_mix table
def get_all_channel_mix():
    with get_session() as session:
        # Execute the raw SQL query
        result = session.execute(query).fetchall()
        return result


channel_mix_data = get_all_channel_mix()


if channel_mix_data:
    for row in channel_mix_data:
        print(row)
else:
    print("No data found in channel_mix table.")
