from sqlmodel import text
from apps.database import get_session

# Raw SQL query to get all data from paid_media table
query = text("SELECT * FROM public.paid_media")


def get_all_paid_media():
    with get_session() as session:
        result = session.execute(query).mappings().all()
        return result


# Example usage
paid_media_data = get_all_paid_media()

# Print results
if paid_media_data:
    print("All Paid Media records:\n")
    for row in paid_media_data:
        print(row)
    print(f"\nTotal records: {len(paid_media_data)}")
else:
    print("No data found in paid_media table.")
