# Arlo Williamsburg — Hotel Forecasting ETL

**Master's Thesis Project**

## Research Question

Can machine learning help urban hotels forecast demand, pricing, and revenue performance more accurately than traditional forecasting methods?

## Project Overview

This project builds a data-driven forecasting tool for Arlo Williamsburg using real internal hotel data. It combines reservation records, daily statistics, rate changes, and guest satisfaction data to produce a clean, model-ready dataset for ML-based forecasting of key hotel KPIs:

- Occupancy rate
- ADR (Average Daily Rate)
- RevPAR (Revenue Per Available Room)
- Room revenue
- Demand patterns and pricing opportunities

## Project Pipeline

```
Raw Hotel Data
↓
ETL Pipeline  (scripts/01–07)
↓
Clean Structured Data  (data/processed/)
↓
Model-Ready Dataset  (data/final/hotel_daily_master.csv)
↓
Machine Learning Forecasting Model  (models/)
↓
Interactive Forecasting Tool / Streamlit App
```

## Data Sources

| Source | Files | Coverage |
|--------|-------|----------|
| Reservation records | `res_main.csv`, `res_daily_24/25.csv` | 2024–2025 |
| Daily stats by booking source | `wburg_daily_stats_source_*.csv` | 2024–2025 |
| Daily stats by market segment | `wburg_daily_stats_by_market_*.csv` | 2024–2025 |
| Daily stats by room type | `wburg_daily_stats_by_rt.csv` | 2024–2025 |
| Rate changes | `Arlo+Williamsburg+RateChange_*.xlsx` | 2024–2025 |
| Guest satisfaction (Medallia) | `medallia_*.xls` | 2024–2025 (half-year chunks) |

## Setup

```powershell
pip install -r requirements.txt
```

## Running the ETL Pipeline

Run scripts in order:

```powershell
python scripts/01_load_inspect_raw_files.py   # inspect raw files
python scripts/02_profile_raw_data.py          # data quality report
python scripts/03_clean_reservations.py        # clean reservation data
python scripts/04_clean_daily_stats.py         # clean daily stats
python scripts/05_clean_rate_changes.py        # clean rate changes
python scripts/06_clean_medallia.py            # clean survey data
python scripts/07_build_master_dataset.py      # build final dataset
```

## Repository Structure

```
arlo_forecasting_etl/
├── app/               # reusable modules (config, loaders, cleaners)
├── data/
│   ├── raw/           # original source files — never modified
│   ├── processed/     # cleaned per-source CSVs
│   └── final/         # merged model-ready datasets
├── models/            # trained model artifacts
├── notebooks/         # exploratory analysis
├── outputs/           # reports and charts
└── scripts/           # numbered ETL steps
```
