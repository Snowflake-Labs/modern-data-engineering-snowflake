## Introduction to Modern Data Engineering with Snowflake

Welcome! This is the companion repository that goes along with Snowflake's **[Introduction to Modern Data Engineering with Snowflake](https://www.coursera.org/learn/data-engineering-snowflake/)** course on Coursera.

#### How to use this repo throughout the course

The course is designed to run inside Snowsight. You connect this repo once as a **Git workspace** in your Snowflake account, and from that point on every module notebook, Streamlit app, and Native App file is available directly in Snowsight.Module 1 walks you through this setup end-to-end. Follow along there before starting the hands-on exercises.

There are expections. Several lessons will make use of Snowflake CLI. Thus, it is also recommended you clone the repo to your local environment:

```bash
git clone https://github.com/Snowflake-Labs/modern-data-engineering-snowflake.git
```

#### How to navigate this repo

Each folder in this repo corresponds to a module in the online course.

* **module-1** – Corresponds to "Module 1: Modern Data Engineering with Snowflake." Companion notebook: `module_1_modern_data_engineering.ipynb`. Also contains `wages_cpi_streamlit_app.py`, which the notebook deploys as a Streamlit in Snowflake app.

* **module-2** – Corresponds to "Module 2: Batch Data Ingestion with Snowflake." Companion notebook: `module_2_batch_data_ingestion.ipynb`. Also contains `load_from_cli_stage.sql` and `sample_orders.csv`, used by the Snowflake CLI lesson.

* **module-3** – Corresponds to "Module 3: Data Transformations with Snowflake." Companion notebook: `module_3_data_transformations.ipynb`. Also contains `hamburg_sales_vs_code.sql`, used by the optional "Data transformations in Visual Studio Code" lesson.

* **module-4** – Corresponds to "Module 4: Delivering Data Products with Snowflake." This module uses two standalone projects rather than a notebook:
  * `streamlit-in-snowflake/streamlit_weather_hamburg.py` – the Streamlit in Snowflake app.
  * `snowflake-native-app/hamburg_weather_native_app/` – the full Snowflake Native App project, deployed with `snow app run`.

* **module-5** – Corresponds to "Module 5: Orchestrating Continuous Data Pipelines with Snowflake." Companion notebook: `module_5_orchestrating_pipelines.ipynb`.

The instructor references the exact folder and file to use throughout the course, so you can follow along.

#### Running a module notebook

Once your Git workspace is set up:

1. In Snowsight, open **Projects → Workspaces** and select the workspace connected to this repo.
2. Open the `module_X_*.ipynb` for the module you're working through.
3. Click **Connect**, choose a warehouse, and run cells top-to-bottom.

The notebooks create their own `wages_cpi` (module 1) and `tasty_bytes` (modules 2–5) databases as needed, so any scratch database works for notebook storage.

#### Prerequisites

* A Snowflake account with `ACCOUNTADMIN` (or a role able to create databases and warehouses).
* **Snowflake CLI**, required for the module 4 Native App exercise. See the [Snowflake CLI installation docs](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation).

#### Reporting issues or errata

If you encounter technical issues or errata as you complete the course (i.e. typos, missing code, broken links, etc.), please report those issues in the course through Coursera, or [open a new issue in this repository](https://github.com/Snowflake-Labs/modern-data-engineering-snowflake/issues/new). Ensure the issue contains sufficient detail so that it can be properly addressed.
