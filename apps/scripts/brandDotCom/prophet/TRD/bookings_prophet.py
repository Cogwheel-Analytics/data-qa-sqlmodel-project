import pandas as pd
from apps.utils.database import get_session
from sqlalchemy import text
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import matplotlib.pyplot as plt
import os
import re


def fetch_all_hotel_data(hotel_code: str) -> pd.DataFrame:
    query = """
        SELECT
            h.code AS hotel_code,
            domain AS domain_name,
            DATE_TRUNC('month', trd.date) AS month,
            SUM(trd.booking) AS booking
        FROM public.top_ref_domain trd
        JOIN public.hotel h ON trd.hotel_id = h.id
        JOIN public.source domain ON trd.domain = domain
        WHERE trd.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '36 month'
          AND trd.date < DATE_TRUNC('month', CURRENT_DATE)
          AND h.code = :hotel_code
          AND h.is_active = TRUE
        GROUP BY h.code, domain, DATE_TRUNC('month', trd.date)
        ORDER BY h.code, domain, month
    """
    with get_session() as session:
        result = session.execute(text(query), {"hotel_code": hotel_code})
        df = pd.DataFrame(
            result.fetchall(), columns=["hotel_code", "domain", "ds", "y"]
        )
    return df


def generate_forecast(df_domain: pd.DataFrame) -> tuple:
    df_domain["ds"] = pd.to_datetime(df_domain["ds"]).dt.tz_localize(None)
    df_domain = df_domain.sort_values("ds")

    if len(df_domain) < 6:
        return None, None, None, None, None

    # Train-test split
    split_index = int(len(df_domain) * 0.75)
    train = df_domain.iloc[:split_index]
    test = df_domain.iloc[split_index:]

    # Fit on full data
    model = Prophet(yearly_seasonality=True)
    model.fit(df_domain)

    # Predict on same data points
    future = df_domain[["ds"]].copy()
    forecast = model.predict(future)

    # Clip yhat to observed bounds
    y_min, y_max = df_domain["y"].min(), df_domain["y"].max()
    forecast["yhat"] = forecast["yhat"].clip(lower=y_min, upper=y_max)

    return model, forecast, train, test, df_domain


def evaluate_forecast(
    df_domain: pd.DataFrame,
    model: Prophet,
    forecast: pd.DataFrame,
    test: pd.DataFrame,
):
    forecast = forecast[forecast["ds"] <= df_domain["ds"].max()]
    forecast_filtered = (
        forecast[["ds", "yhat"]].set_index("ds").join(test.set_index("ds"))
    )
    forecast_filtered.dropna(inplace=True)

    if forecast_filtered.empty:
        return None

    mae = mean_absolute_error(forecast_filtered["y"], forecast_filtered["yhat"])
    rmse = np.sqrt(
        mean_squared_error(forecast_filtered["y"], forecast_filtered["yhat"])
    )
    mape = (
        np.mean(
            np.abs(
                (forecast_filtered["y"] - forecast_filtered["yhat"])
                / forecast_filtered["y"]
            )
        )
        * 100
    )
    return mae, rmse, mape


def plot_forecast(
    model: Prophet,
    forecast: pd.DataFrame,
    df_domain: pd.DataFrame,
    test: pd.DataFrame,
    hotel_code: str,
    domain: str,
    output_dir: str,
):
    fig = model.plot(forecast)
    plt.scatter(
        df_domain["ds"], df_domain["y"], color="black", label="Actual", zorder=5
    )

    # Mark train/test split
    split_index = int(len(df_domain) * 0.75)
    plt.axvline(
        df_domain["ds"].iloc[split_index],
        color="red",
        linestyle="--",
        label="Train/Test Split",
    )

    # X-axis formatting
    months = pd.date_range(df_domain["ds"].min(), df_domain["ds"].max(), freq="MS")
    ax = fig.gca()
    ax.set_xticks(months)
    ax.set_xticklabels(
        [d.strftime("%b %Y") for d in months], rotation=45, ha="right", fontsize=8
    )
    plt.title(f"Forecast vs Actual - {hotel_code} - {domain}")
    plt.legend()

    # Save plot
    os.makedirs(output_dir, exist_ok=True)
    safe_domain = re.sub(r"[^\w\-_.]", "_", domain)
    filename = f"{output_dir}/{safe_domain}.png"
    fig.savefig(filename, bbox_inches="tight")
    plt.close()
    print(f"Saved: {filename}")


def forecast_and_plot(
    df_domain: pd.DataFrame, hotel_code: str, domain: str, output_dir: str
):
    model, forecast, train, test, df_domain = generate_forecast(df_domain)
    if model is None:
        print(f"Skipping {hotel_code} - {domain} (not enough data)")
        return

    metrics = evaluate_forecast(df_domain, model, forecast, test)
    if not metrics:
        print(f"Skipping {hotel_code} - {domain} (no overlapping forecast/test data)")
        return

    mae, rmse, mape = metrics
    print(f"\n{hotel_code} - {domain}")
    print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%")

    plot_forecast(model, forecast, df_domain, test, hotel_code, domain, output_dir)


def main():
    hotel_code = "BOSFRUP"
    df = fetch_all_hotel_data(hotel_code)
    if df.empty:
        print("No data found.")
        return

    grouped = df.groupby("hotel_code")

    for hotel_code, hotel_df in grouped:
        print(f"\n=== Processing {hotel_code} ===")
        output_dir = f"forecast_plots/TRD/Bookings/{hotel_code}"

        for domain in hotel_df["domain"].unique():
            df_domain = hotel_df[hotel_df["domain"] == domain].copy()
            forecast_and_plot(df_domain, hotel_code, domain, output_dir)


if __name__ == "__main__":
    main()
