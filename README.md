# Intersecting Predators: An Integrated Database of Shark and Crocodilian Aggression & Population Trends

**Status:** Work in Progress  
**Stretch Goal:** Movement data integration planned for future phases.

---

## Executive Summary

This project builds a centralized, queryable database integrating shark and crocodilian (alligator & crocodile) attack data and species’ population trends.  
**Central Question:**  
_Do shark and crocodilian habitats intersect more than commonly assumed, and does that overlap correspond to increased aggression or influence species’ population trends?_

By combining these datasets, the project enables spatial and temporal analysis of predator overlap, aggression, and population health, supporting ecological and behavioral research.

---

## Data Sources

- **International Shark Attack Files (ISAF)**  
  - [Global Shark Attack Locations](https://www.floridamuseum.ufl.edu/shark-attacks/trends/location/world/)
- **Global Shark Attack Database (CSV Download)**
  - [Opendatasoft Shark Attacks](https://public.opendatasoft.com/explore/dataset/global-shark-attack/export/?disjunctive.country&disjunctive.area&disjunctive.activity&sort=-original_order)
- **Crocattack.org**  
  - [Crocodile/Alligator Attack API & Database](https://crocattack.org/database/)
- **IUCN Red List**
  - [Crocodilian Population Trends](https://www.iucnredlist.org/search?permalink=0894a85e-f70f-4b81-bb47-07a80bdd4786)
  - [Shark Population Trends](https://www.iucnredlist.org/search?permalink=f095bdae-5a22-409c-af51-82353602ea89)
- **Movebank.org** (Stretch Goal)
  - Movement tracking data (integration planned for future)

---

## Technical Architecture

| Technology      | Role                       | Purpose                                                         |
|-----------------|---------------------------|-----------------------------------------------------------------|
| Python Scripts  | Data Ingestion            | Extract data (API calls, CSV downloads, web scraping)           |
| MinIO           | Raw Data Lake             | Store raw JSON/CSV files                                        |
| Snowflake       | Analytical Engine         | Queryable storage, transformations                              |
| DBT             | Data Transformation       | ELT, schema alignment, cleaning, metrics computation            |
| Airflow         | Orchestration             | Automate and schedule pipeline                                  |
| FastAPI + Swagger | Semantic Layer/API      | Documented endpoints, external access to metrics                |
| Metabase/Streamlit | BI & Visualization     | Dashboards for overlap, aggression, population trends           |

---

## Current Pipeline Workflow

1. **Automated Ingestion**  
   - Airflow triggers Python scripts to fetch attack and population data (see above).
   - Raw data stored in MinIO.

2. **Bronze Layer Creation**  
   - Load raw files into Snowflake structured tables.

3. **Transformation & Modeling**  
   - DBT normalizes, aligns schema, and cleans data (esp. date, lat/long).
   - Derived metrics: habitat overlap indices, aggression density, mortality rates.

4. **API Access**  
   - FastAPI exposes endpoints for querying metrics, documented via Swagger.

5. **Visualization**  
   - Metabase or Streamlit dashboards allow interactive exploration.

---

## Actionable Insights

- **Ecological Research**: Reveals predator interaction zones.
- **Predator Dynamics**: Explore aggression patterns in overlap regions.
- **Population Monitoring**: Track health trends in stressed regions.
- **Academic Research**: Unified data for hypothesis testing and analysis.

---

## Project Journal & Development Lessons

- **Webscraping**: BeautifulSoup chosen for static HTML; Selenium unnecessary.
- **APIs**: Network tab in browser helped discover undocumented endpoints.
- **Data Wrangling**: Real-world data is messy; manual download often required (IUCN, population CSVs).
- **Pivoting**: Movement data integration deferred due to availability challenges; attack and population data used as proxies for habitat overlap.
- **CSV Handling**: Custom delimiter and quoting logic required for shark attack data.
- **Data Cleaning**: Used SQL functions (`coalesce`, `try_to_date`) for robust date handling; maintained raw and cleaned columns for transparency.
- **Python Imports**: Managed module dependencies via absolute paths and considered tools like Poetry for future refactoring.
- **Pipeline Changes**: Directly ingest CSVs via HTTP requests where possible to minimize scraping overhead.

---

## Getting Started

> **Note:** This pipeline is under active development. Instructions below reflect current workflow.

### Prerequisites

- Python 3.10+
- Access to MinIO, Snowflake, Airflow, DBT
- (Optional) FastAPI, Metabase/Streamlit

### Setup

1. **Clone the Repository**

   ```bash
   git clone https://github.com/NSS-Data-Engineering-May2025/shark-gator-capstone.git
   cd shark-gator-capstone
   ```

2. **Install Python Dependencies**

   ```
   pip install -r requirements.txt
   ```

3. **Configure MinIO and Snowflake connections**  
   Update `.env` or config files as needed.

4. **Run Data Ingestion Scripts**

   - See `scripts/` directory for ingestion scripts.
   - Example:
     ```bash
     python scripts/ingest_shark_attacks.py
     python scripts/ingest_croc_attacks.py
     ```

5. **Trigger Pipeline**
   - Set up and run Airflow DAGs for end-to-end automation.

6. **Run DBT Transformations**
   - See `dbt/` project for models and tests.

7. **Start FastAPI (Optional)**
   - `uvicorn api:app --reload`

8. **View Dashboards**
   - Connect Metabase or Streamlit to Snowflake/your API.

---

## Planned Features & Stretch Goals

- Integrate movement data (Movebank) to enable direct spatial analysis.
- Expand population trend data coverage.
- Advanced visualizations (interactive maps, aggression heatmaps).
- Automated quality checks for all ingested datasets.

---

## Contributing

Pull requests welcome!  
Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

[MIT](LICENSE)

---

## Contact

Lead: Helen Esterman  
For questions, suggestions, or collaboration, open an issue or email [your-email@example.com].

---

## Acknowledgments

- Florida Museum of Natural History (ISAF)
- Crocattack.org
- IUCN Red List
- Movebank.org
