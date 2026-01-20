# ⚽ FPL Player Points Prediction

Predicting Fantasy Premier League (FPL) player points for upcoming gameweeks using historical performance, playing-time signals, and fixture context.
This project focuses on building a **time-aware, leakage-free machine learning pipeline** for week-ahead forecasting.

---

## 🎯 Project Goals

* Predict **expected FPL points per player** for the next gameweek
* Model **playing time probability and expected minutes**
* Incorporate **fixture difficulty and team strength**
* Evaluate models using **time-based validation**
* Provide ranked recommendations for:

  * Captain picks
  * Differential players

This is built as a reproducible ML pipeline.

---

## 🧠 Problem Formulation

Each data point represents:

> **One player, in one season, for one gameweek — using only information available before that gameweek starts.**

**Prediction target:**

```
Expected FPL points in Gameweek (t + 1)
```

---

## 📊 Data Sources

### Primary

* **Official FPL API**

  * Player metadata
  * Minutes, points, bonus, BPS
  * Team and fixture information
  * [https://fantasy.premierleague.com/api/](https://fantasy.premierleague.com/api/)

* **Understat**

  * Expected goals (xG), expected assists (xA)
  * Shot locations and volume
  * Team attacking and defensive metrics

### Optional / Future

* FBref advanced stats
* Team Elo or SPI ratings
* Bookmaker implied goal odds

---

## 🧱 Data Architecture

The dataset is organized as a **player–gameweek panel**.

### Core Modeling Table

**Table: `player_gw_features`**

Primary key:

```
(player_id, season, gameweek)
```

Each row contains:

* Player metadata
* Fixture context
* Lagged performance features
* Playing time indicators
* Team and opponent strength
* Risk and availability flags
* Training targets (shifted forward)

All features are strictly **lagged** to prevent data leakage.

---

### Supporting Tables

#### `player_match_stats_raw`

Raw per-match player statistics.

Includes:

* minutes, total_points
* goals, assists, bonus, BPS
* xG, xA, shots, key passes

Used to compute rolling and lagged features.

---

#### `team_match_stats`

Team-level match metrics.

Includes:

* xG, xGA
* goals scored / conceded
* clean sheet indicator

Used for team form and opponent strength features.

---

#### `fixtures`

Match scheduling and difficulty context.

Includes:

* team vs opponent
* home/away
* fixture difficulty rating
* days of rest
* blank and double gameweeks

---

## ⏱ Feature Engineering Principles

* Only historical data is used for feature creation
* Rolling windows: 3-match and 5-match aggregates
* Targets are created using forward shifts
* No future match information is allowed in features

This setup mirrors real-world forecasting constraints.

---

## 🤖 Modeling Strategy

### Baseline

* Rolling average of past FPL points

### ML Models

* Gradient boosting (LightGBM / XGBoost)
* Poisson-based goal involvement models
* Two-stage approach:

  1. Predict expected minutes
  2. Predict points per 90

Final prediction:

```
Expected Points = Expected Minutes × Points per 90
```

---

## 📈 Evaluation

Evaluation uses **time-based splits**, never random sampling.

Examples:

* Train: GW 1–25 → Test: GW 26
* Rolling backtests across the season

Metrics:

* MAE / RMSE on predicted points
* Rank correlation for captain selection
* Calibration of expected minutes

---

## 🗂 Repository Structure

```
fpl-points-predictor/
│
├── data/
│   ├── raw/
│   ├── processed/
│
├── src/
│   ├── ingest/        # API & scraping
│   ├── features/      # feature engineering
│   ├── models/        # training & inference
│   ├── evaluation/    # backtesting
│
├── notebooks/
│   ├── EDA.ipynb
│   ├── Modeling.ipynb
│
├── app/
│   └── predict_next_gw.py
│
└── README.md
```

---

## 🚀 Future Improvements

* Probabilistic prediction (quantiles, uncertainty)
* Captaincy expected value modeling
* Transfer optimization simulation
* Weekly automated retraining and inference
* Web dashboard for predictions



