# Microsoft Fabric Ride-Sharing Data Platform

End-to-end data engineering project on **Microsoft Fabric**. Historical (batch) and real-time streaming ride-sharing data flow through a **Bronze → Staging → Silver → Gold** medallion-style architecture, is validated by a quality gate, modeled as a **star schema with SCD Type 2**, served through a **Direct Lake** semantic model, and consumed in an interactive Power BI report.

> \*\*Note:\*\* The data is \*\*synthetic\*\* and ride-sharing themed. This is a portfolio project, not an official Uber system.

The focus is **data engineering**. The report is a thin consumption layer that proves the pipeline works end to end.

\---

## Architecture

!\[Architecture Diagram](docs/images/architecture\_diagram.png)

*Solid arrows: data flow. Dashed arrows: orchestration and monitoring.*

|Layer|Responsibility|
|-|-|
|**Ingestion**|**Batch:** historical JSON loaded over HTTP by a Fabric pipeline. **Streaming:** Python simulator → Event Hub → Eventstream|
|**Bronze**|Raw, source-aligned data: bulk (historical) and streaming kept separate, plus reference/mapping tables|
|**Staging**|Combines batch and streaming data in the Lakehouse|
|**Silver**|Clean, standardize, enrich; build the ride-level `rides\_obt` and change-detection objects|
|**Gold**|Governed dimensional model: `fact\_rides` plus dimensions|
|**Quality gate**|Block bad data from reaching analytics|
|**Semantic model**|Central business definitions (DAX) over Gold via Direct Lake|
|**Report**|Single-page executive view|

\---

## Tech Stack

Event Hub · Microsoft Fabric (Eventstream, Data Factory pipelines, Lakehouse, OneLake, Delta tables) · Apache Spark / PySpark notebooks · T-SQL stored procedures · Direct Lake · Power BI semantic model · DAX · Fabric Monitoring Hub

\---

## Key Features

* **Medallion architecture** with clear separation: Bronze is source-oriented, Silver is business-refined, Gold is analytics-oriented
* **Incremental processing:** new rides propagate through all layers without a full rebuild
* **SCD Type 2** on driver, passenger, city, and vehicle-type dimensions
* **Surrogate keys** separating source business IDs from warehouse keys
* **Orchestration:** a scheduled Fabric pipeline runs ingestion, Silver transforms, dimension loads, fact load, and validation in order
* **Data-quality validation:** duplicates, nulls, referential integrity, business rules, fact/dimension consistency
* **Direct Lake** semantic model: no duplicated Import copy of Gold

\---

## Gold Layer: Star Schema

```mermaid
erDiagram
    DIM\_DATE ||--o{ FACT\_RIDES : DateKey
    DIM\_DRIVER ||--o{ FACT\_RIDES : DriverKey
    DIM\_PASSENGER ||--o{ FACT\_RIDES : PassengerKey
    DIM\_CITY ||--o{ FACT\_RIDES : "PickupCityKey / DropoffCityKey"
    DIM\_VEHICLE\_TYPE ||--o{ FACT\_RIDES : VehicleTypeKey
    DIM\_VEHICLE\_MAKE ||--o{ FACT\_RIDES : VehicleMakeID
    DIM\_PAYMENT\_METHOD ||--o{ FACT\_RIDES : PaymentMethodID
    DIM\_RIDE\_STATUS ||--o{ FACT\_RIDES : RideStatusID
    DIM\_CANCELLATION\_REASON ||--o{ FACT\_RIDES : CancellationReasonID
```

* **`fact\_rides`** holds ride events and measures: fares (base, distance, time, surge, tip, total), distance, duration, ratings, and timestamps.
* `dim\_city` plays two roles (pickup and dropoff). The report uses **pickup city** as its active relationship.
* Dimensions load **before** the fact table so keys exist when the fact is loaded (`usp\_load\_dim\_\*` → `usp\_load\_fact\_rides`).

### SCD Type 2 pattern

Tracked dimensions carry `SurrogateKey`, `BusinessKey`, `EffectiveFrom`, `EffectiveTo`, `IsCurrent`.

```
Entity changes → current row expires (EffectiveTo = change time, IsCurrent = false)
              → new version inserted (new surrogate key, IsCurrent = true)
```

Historical facts stay linked to the dimension version that was valid at the time.

\---

## Pipeline Flow

```
Ingestion → Bronze → Staging → Silver → Gold dimensions → Gold fact → Validation → Success
```

Orchestrated and scheduled by a Fabric Data Factory pipeline. Failed validation fails the run.

\---

## Data Quality and Troubleshooting

### 1\. NULL mapping caught by Gold constraint

* **Symptom:** Gold dimension load failed.
* **Root cause:** `map\_cancellation\_reasons` had `ID 4 → NULL` in the source. It propagated unchanged through Bronze and Silver, but `dim\_cancellation\_reason.CancellationReason` is `NOT NULL`.
* **Fix:** Gold procedure maps the NULL to **`Not Applicable`**.
* **Principle:** Bronze/Silver keep source fidelity; Gold exposes a business-friendly value. The constraint stopped bad data from entering silently.

### 2\. Spark capacity contention

* **Symptom:** `TooManyRequestsForCapacity` (HTTP 430), failed to create Livy session.
* **Root cause:** Fabric Spark hit its concurrent compute limit. Not a logic bug.
* **Resolution:** Diagnosed in **Monitoring Hub**, stopped unnecessary Spark workloads, reran.

\---

## Semantic Model and Report

**DAX measures** are grouped as Ride, Revenue, Surge, Trip, Driver/Passenger, MoM Trend, and Quality KPIs. Examples: completion rate, total revenue, surge revenue %, rides per driver, revenue growth % MoM.

**Executive Overview report:** KPI cards, monthly revenue trend, top pickup cities, revenue by vehicle type, ride-status breakdown, slicers (date, pickup city, vehicle type), tooltips, cross-filtering, and a bookmark-based reset button.

<!-- Add screenshot: !\[Executive Report](docs/images/executive\_report.png) -->

\---

## End-to-End Validation

After the initial load, new rides were generated and traced through every stage:

1. New records arrive in **Bronze**
2. Processed in **Silver**
3. `fact\_rides` row count increases in **Gold**
4. Validation passes
5. **Direct Lake** model reflects the latest Gold state (checked with a DAX `COUNTROWS(fact\_rides)` diagnostic to isolate Gold vs. model vs. report)
6. Report shows the new data

\---



## Skills Demonstrated

Medallion design · dimensional modeling · SCD Type 2 · incremental loading · pipeline orchestration · data-quality engineering · root-cause troubleshooting · Direct Lake / DAX semantic modeling · Fabric operations and monitoring

