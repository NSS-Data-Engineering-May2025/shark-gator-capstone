# Shark Gator Database
_A Centralized Database of Shark and Crocodilian Attacks and Population Trends_

**Status:** MVP Complete  
**Next Step:** Movement data enrichment (stretch goal)

---

## Project Overview

Shark Gator Database is a modular data engineering pipeline centralizing global shark and crocodilian attack records, mapped to population trends.  
Designed for anyone's research, this project demonstrates advanced ELT architecture, robust data cleaning, and automated analytics delivery via Metabase dashboards.

---

## Data Sources

- **Crocattack.org API**  
  - Global records of crocodile & alligator attacks ([API](https://crocattack.org/database/))
- **Global Shark Attack File (GSAF) CSV Download**  
  - Shark attack records from the 1500s to present ([GSAF CSV](https://public.opendatasoft.com/explore/dataset/global-shark-attack/table/?disjunctive.country&disjunctive.area&disjunctive.activity&sort=-original_order))
- **IUCN Red List CSV Downloads**  
  - Population, conservation status, & extinction risk for sharks and crocodilians ([Sharks](https://www.iucnredlist.org/search?permalink=f095bdae-5a22-409c-af51-82353602ea89), [Crocodilians](https://www.iucnredlist.org/search?permalink=0894a85e-f70f-4b81-bb47-07a80bdd4786))

---

## Architecture & Technical Highlights

**Pipeline: ELT Medallion Architecture, Automated by Airflow**

- **Raw Ingestion (Python & Airflow)**
  - API and CSV data are ingested and dropped into a raw MinIO bucket.
- **Bronze Layer (Snowflake)**
  - Raw files loaded as "bronze" tables for persistent storage.
- **Silver Layer (DBT: SQL & Python)**
  - Aggressive cleaning, normalization, and standardization.
  - Used regex, fuzzy matching, and other techniques to resolve messy columns (e.g., species, location, dates).
  - Standardized country/state names, created flags, and handled nulls with purpose.
- **Gold Layer (DBT SQL)**
  - Joins, aggregations, and analytics-ready tables.
  - Linked attack records to population trends for both sharks and crocodilians.
- **Visualization (Metabase)**
  - Dashboards connect to Snowflake for interactive insights.

**Orchestration:**
- Automated via a robust Airflow DAG; each major step also has a dedicated DAG for manual testing and debugging.
- Dev container used for environment consistency and lightweight iteration.

**Testing & CI/CD:**
- Pytest and GitHub Actions for error handling, unit tests, and pipeline reliability.

---

## Key Wins & Engineering Skills

- **Orchestration:** Overcame Airflow configuration and environment/versioning challenges, and automated the full pipeline.
- **Data Cleaning:** Tackled messy real-world data using a mix of SQL, Python (Jupyter, pandas), and regex; discovered reusable fuzzy matching formulas for deduplication and standardization (e.g., “Florda” → “Florida”).
- **EDA Efficiency:** Leveraged DuckDB and DataGrip for offline exploratory analysis, saving Snowflake query costs.
- **CI/CD Integration:** Combined GitHub Actions and pytest to ensure effective error handling and pipeline reliability.
- **Problem Solving:** Pivoted quickly when faced with inaccessible data (e.g., movement data), found better sources, and documented all major architectural decisions.
- **Python Module Management:** Resolved persistent import issues and learned best practices for packaging and dependency management.

---

## Insights Delivered

- Shark vs. Gator Attack Counts by Country (Where Both Occur)
- Shark vs. Gator Attacks Over Time (Years with Both Present)
- Most Aggressive Sharks and Their Population Trends
- Most Aggressive Crocodilians and Their Population Trends
- Unified data platform for hypothesis testing regarding shark attacks, crocodilian attacks, and the population trends associated with each animal

---

## Getting Started

### Prerequisites

- Python 3.10+
- Airflow
- MinIO
- Snowflake account
- DBT
- Metabase

### Setup

1. **Clone the Repository**
    ```bash
    git clone https://github.com/NSS-Data-Engineering-May2025/shark-gator-capstone.git
    cd shark-gator-capstone
    ```
2. **Install Python Dependencies**
    ```bash
    pip install -r requirements.txt
    ```
3. **Configure Connections**
    - Update `.env` or config files for MinIO, Snowflake, Airflow.
4. **Run Airflow DAG**
    - Trigger the main pipeline DAG for full automation.
    - Use manual DAGs for isolated debugging.
5. **Run DBT**
    - DBT project includes silver/gold models and tests.
6. **Explore Insights**
    - Connect Metabase to Snowflake and view dashboards.

---

## Stretch Goals & Future Work

- Add semantic data layer (Cube.js) for more efficient analysis and cost control.
- Strengthen orchestration: upgrade Airflow (v3, CeleryExecutor, Postgres) or explore Prefect/Dagster.
- Enhance messy columns with lookup tables & indexing for faster joins.
- Expand dataset with Movebank movement data for deeper insights.
- Integrate GitHub Secret Keys into CI/CD GitHub Actions for secure, automated DBT Testing.
- Expand dataset coverage and improve gold layer scalability.

---

## Contact

Helen Esterman  
Questions, suggestions, or collaboration: open an issue or email helenesterman99@gmail.com or connect with me on LinkedIn- www.linkedin.com/in/helenesterman

---

## Acknowledgments

- Crocattack.org (API)
- Global Shark Attack File (CSV)
- IUCN Red List

---
