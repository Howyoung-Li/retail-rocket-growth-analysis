# E-commerce Growth Analytics (Spark + Power BI)

[中文说明](README.md)

This project builds an end-to-end growth analytics pipeline on e-commerce event data.
User behavior logs are processed with **PySpark**, and key metrics are visualized in **Power BI**
to support growth analysis and business decision-making.

---

## Data & Pipeline

- Raw data: Retail Rocket user event logs
- Processing: PySpark (WSL2)
- Granularity: Daily

Metrics include:
- Daily active users (DAU)
- Conversion funnel (view → add-to-cart → transaction)
- Item-level conversion diagnostics
- Cohort-based retention (D1, D7 rolling)
- All metrics are dynamically recalculated based on the selected date range using a global date slicer.

---

## Figure A: Growth Overview

![Overview](dashboard/overview.png)

Provides a snapshot of user scale and conversion efficiency over the selected period.

---

## Figure B: Trend Analysis

![Trends](dashboard/trends.png)

DAU and purchase conversion rate trends help distinguish
traffic-driven growth from efficiency-driven growth.

---

## Figure C: User Funnel

![Funnel](dashboard/funnel.png)

Identifies the primary drop-off stage in the user conversion process.

---

## Figure D: Item-Level Opportunities

![Items](dashboard/items.png)

A scatter plot of traffic vs. conversion highlights:
- High-traffic, low-conversion items (optimization candidates)
- High-conversion, low-traffic items (scaling opportunities)

---

## Figure E: Cohort Retention

![Retention](dashboard/retention.png)

D1 and D7 rolling retention are used to evaluate user quality over time.

---

## Tech Stack

- PySpark / Spark SQL
- Power BI
- Python
- WSL2

