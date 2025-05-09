import csv
import os
from typing import Dict, List


def export_hotel_months_to_csv(
    hotel_months: Dict[str, List[str]],
    filename: str,
    folder: str = "",
) -> None:
    # Create full path
    if folder:
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, filename)
    else:
        path = filename

    with open(path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Hotel Code", "Missing Months"])

        for hotel_code, months in hotel_months.items():
            months_str = str(months)
            writer.writerow([hotel_code, months_str])

    print(f"CSV exported to: {path}")
