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
            sr.name AS source_name,
            DATE_TRUNC('month', st.date) AS month,
            SUM(st.visits) AS visits
        FROM public.source_traffic st
        JOIN public.hotel h ON st.hotel_id = h.id
        JOIN public.source sr ON st.source_id = sr.id
        WHERE st.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '36 month'
          AND st.date < DATE_TRUNC('month', CURRENT_DATE)
          AND h.code = :hotel_code
          AND h.is_active = TRUE
        GROUP BY h.code, sr.name, DATE_TRUNC('month', st.date)
        ORDER BY h.code, sr.name, month
    """
    with get_session() as session:
        result = session.execute(text(query), {"hotel_code": hotel_code})
        df = pd.DataFrame(
            result.fetchall(), columns=["hotel_code", "source", "ds", "y"]
        )
    return df


def generate_forecast(df_source: pd.DataFrame) -> tuple:
    df_source["ds"] = pd.to_datetime(df_source["ds"]).dt.tz_localize(None)
    df_source = df_source.sort_values("ds")

    if len(df_source) < 6:
        return None, None, None, None, None

    # Train-test split
    split_index = int(len(df_source) * 0.75)
    train = df_source.iloc[:split_index]
    test = df_source.iloc[split_index:]

    # Fit on full data
    model = Prophet(yearly_seasonality=True)
    model.fit(df_source)

    # Predict on same data points
    future = df_source[["ds"]].copy()
    forecast = model.predict(future)

    # Clip yhat to observed bounds
    y_min, y_max = df_source["y"].min(), df_source["y"].max()
    forecast["yhat"] = forecast["yhat"].clip(lower=y_min, upper=y_max)

    return model, forecast, train, test, df_source


def evaluate_forecast(
    df_source: pd.DataFrame,
    model: Prophet,
    forecast: pd.DataFrame,
    test: pd.DataFrame,
):
    forecast = forecast[forecast["ds"] <= df_source["ds"].max()]
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
    df_source: pd.DataFrame,
    test: pd.DataFrame,
    hotel_code: str,
    source: str,
    output_dir: str,
):
    fig = model.plot(forecast)
    plt.scatter(
        df_source["ds"], df_source["y"], color="black", label="Actual", zorder=5
    )

    # Mark train/test split
    split_index = int(len(df_source) * 0.75)
    plt.axvline(
        df_source["ds"].iloc[split_index],
        color="red",
        linestyle="--",
        label="Train/Test Split",
    )

    # X-axis formatting
    months = pd.date_range(df_source["ds"].min(), df_source["ds"].max(), freq="MS")
    ax = fig.gca()
    ax.set_xticks(months)
    ax.set_xticklabels(
        [d.strftime("%b %Y") for d in months], rotation=45, ha="right", fontsize=8
    )
    plt.title(f"Forecast vs Actual - {hotel_code} - {source}")
    plt.legend()

    # Save plot
    os.makedirs(output_dir, exist_ok=True)
    safe_source = re.sub(r"[^\w\-_.]", "_", source)
    filename = f"{output_dir}/{safe_source}.png"
    fig.savefig(filename, bbox_inches="tight")
    plt.close()
    print(f"Saved: {filename}")


def forecast_and_plot(
    df_source: pd.DataFrame, hotel_code: str, source: str, output_dir: str
):
    model, forecast, train, test, df_source = generate_forecast(df_source)
    if model is None:
        print(f"Skipping {hotel_code} - {source} (not enough data)")
        return

    metrics = evaluate_forecast(df_source, model, forecast, test)
    if not metrics:
        print(f"Skipping {hotel_code} - {source} (no overlapping forecast/test data)")
        return

    mae, rmse, mape = metrics
    print(f"\n{hotel_code} - {source}")
    print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%")

    plot_forecast(model, forecast, df_source, test, hotel_code, source, output_dir)


def main():
    hotel_code = "PHLCVHX"
    df = fetch_all_hotel_data(hotel_code)
    if df.empty:
        print("No data found.")
        return

    grouped = df.groupby("hotel_code")

    for hotel_code, hotel_df in grouped:
        print(f"\n=== Processing {hotel_code} ===")
        output_dir = f"forecast_plots/sourceTraffic/Visits/{hotel_code}"

        for source in hotel_df["source"].unique():
            df_source = hotel_df[hotel_df["source"] == source].copy()
            forecast_and_plot(df_source, hotel_code, source, output_dir)


if __name__ == "__main__":
    main()
