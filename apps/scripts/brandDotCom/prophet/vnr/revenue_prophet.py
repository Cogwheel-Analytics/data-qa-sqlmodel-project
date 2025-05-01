from sqlmodel import text
from apps.database import get_session
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import os
import re

TARGET_HOTEL_CODE = "BOSFRUP"


def fetch_monthly_revenue(hotel_code: str):
    query = text(
        """
        SELECT
            h.code AS hotel_code,
            DATE_TRUNC('month', vnr.date) AS month,
            SUM(vnr.revenue) AS revenue
        FROM public.visit_revenue vnr
        JOIN public.hotel h ON vnr.hotel_id = h.id
        WHERE vnr.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '36 month'
        AND vnr.date < DATE_TRUNC('month', CURRENT_DATE)
        AND h.code = :hotel_code
        AND h.is_active = TRUE
        GROUP BY h.code, DATE_TRUNC('month', vnr.date)
        ORDER BY month
        """
    )
    with get_session() as session:
        result = session.execute(query, {"hotel_code": hotel_code})
        df = pd.DataFrame(
            result.fetchall(),
            columns=["hotel_code", "month", "revenue"],
        )
    return df


def evaluate_forecast(prophet_df, model, horizon_months=3):
    train_df = prophet_df[:-horizon_months]
    test_df = prophet_df[-horizon_months:]

    model.fit(train_df)
    future = model.make_future_dataframe(periods=horizon_months, freq="MS")
    forecast = model.predict(future)

    forecast_df = forecast[["ds", "yhat"]].merge(test_df, on="ds", how="inner")

    mae = (forecast_df["yhat"] - forecast_df["y"]).abs().mean()
    rmse = ((forecast_df["yhat"] - forecast_df["y"]) ** 2).mean() ** 0.5
    mape = (
        abs((forecast_df["yhat"] - forecast_df["y"]) / forecast_df["y"])
    ).mean() * 100

    return mae, rmse, mape


def analyze_revenue_trend(df, hotel_code: str):
    output_dir = f"forecast_plots/visits_revenue/{hotel_code}"
    os.makedirs(output_dir, exist_ok=True)

    prophet_df = (
        df.rename(columns={"month": "ds", "revenue": "y"})
        .sort_values("ds")
        .assign(ds=lambda df: df["ds"].dt.tz_localize(None))
    )

    if len(prophet_df) < 6:
        print(f"Skipping {hotel_code} (not enough data)")
        return

    # Evaluation using a temporary model instance
    eval_model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
    )
    mae, rmse, mape = evaluate_forecast(prophet_df, eval_model)

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
    )
    print(
        f"Forecast Evaluation for {hotel_code}:\n"
        f"MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%\n"
    )

    model.fit(prophet_df)
    future = model.make_future_dataframe(periods=3, freq="MS")
    forecast = model.predict(future)

    fig = model.plot(forecast)
    ax = fig.gca()

    # Set y-axis
    full_revenue = pd.concat(
        [
            prophet_df[["ds", "y"]],
            forecast[["ds", "yhat"]].rename(columns={"yhat": "y"}),
        ]
    )
    global_max = full_revenue["y"].max() * 1.2
    ax.set_ylim(0, global_max)

    ax.scatter(prophet_df["ds"], prophet_df["y"], color="red", label="Actual Revenue")
    for x, y in zip(prophet_df["ds"], prophet_df["y"]):
        ax.annotate(
            f"{y:,.0f}",
            (x, y),
            textcoords="offset points",
            xytext=(0, 5),
            ha="center",
            fontsize=8,
            color="black",
        )

    future_forecast = forecast[forecast["ds"] > prophet_df["ds"].max()]
    ax.scatter(
        future_forecast["ds"],
        future_forecast["yhat"],
        color="blue",
        marker="x",
        label="Forecasted Revenue",
    )
    for x, y in zip(future_forecast["ds"], future_forecast["yhat"]):
        ax.annotate(
            f"{y:,.0f}",
            (x, y),
            textcoords="offset points",
            xytext=(0, 5),
            ha="center",
            fontsize=8,
            color="blue",
        )

    fig.autofmt_xdate()
    plt.title(f"{hotel_code} - Visit Revenue Forecast")
    plt.legend()
    fig.tight_layout()
    fig.savefig(f"{output_dir}/visit_revenue_forecast.png")
    plt.close()

    print(f"Saved forecast plot for visit revenue: {hotel_code}")


def main():
    df = fetch_monthly_revenue(TARGET_HOTEL_CODE)
    if not df.empty:
        analyze_revenue_trend(df, TARGET_HOTEL_CODE)
    else:
        print(f"No visit revenue data found for hotel: {TARGET_HOTEL_CODE}")


if __name__ == "__main__":
    main()
