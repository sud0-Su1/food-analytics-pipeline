# Amazon–Snowflake Food Delivery AI Pipeline

A batch analytics project for food delivery data. It loads raw food, restaurant, customer, order, and review data into Snowflake, cleans and models it with dbt, schedules the workflow with Airflow, and provides local language-model interfaces for review search, review classification, and natural-language questions over analytics tables.

The repository combines an Amazon S3/Snowflake-style ingestion setup with a Zomato-named demo data model. The project files assume that the Snowflake database, raw tables, warehouse, role, stage, and Airflow Snowflake connection already exist or are configured outside this repository. The Airflow DAG refers to an external stage named `ZOMATO.RAW.ZOMATO_RAW_STAGE`; this repository does not create the S3 bucket, stage, storage integration, or raw-table schemas.

## What it does

1. Airflow copies staged CSV data from Snowflake's raw stage into the `ZOMATO.RAW` tables.
2. dbt builds cleaned staging views and analytics marts in Snowflake.
3. A Python task classifies new reviews and writes the labels to `ZOMATO.AI.REVIEW_ENRICHED`.
4. Two Streamlit apps support semantic search over reviews and natural-language querying of Snowflake marts.

## Architecture

```text
CSV files / upstream data
          |
          v
S3 bucket and Snowflake external stage (configured outside this repository)
          |
          v
Airflow `zomato_batch`: COPY INTO ZOMATO.RAW.*
          |
          v
dbt staging views: ZOMATO.STAGING.STG_*
          |
          +--> dbt marts: ZOMATO.MARTS.*
          |
          +--> review enrichment: ZOMATO.AI.REVIEW_ENRICHED
                    |
                    +--> review-insights SQL model (draft at repository root)

Streamlit apps --> Ollama (local model) and Snowflake
```

The dbt schema macro uses each model's configured custom schema directly. With the project settings, staging models go to `STAGING` and marts go to `MARTS`, independent of the profile's default `RAW` schema.

For a longer walkthrough of the data relationships, dimensional model, orchestration, and AI features, see [Architecture and AI concepts](docs/architecture-and-ai.md).

## Repository layout

| Path | Purpose |
| --- | --- |
| `airflow/dags/food_batch.py` | Daily Airflow batch DAG and its ingestion, dbt, and enrichment tasks. |
| `airflow/docker-compose.yml`, `airflow/dockerfile` | Local Airflow 3, PostgreSQL metadata database, and Streamlit services. |
| `fdp/dbt_project.yml`, `fdp/profiles.yml` | dbt project and Snowflake profile configuration. |
| `fdp/models/staging/` | Source declarations, cleaned views, and staging tests. |
| `fdp/models/marts/` | Dimensions, facts, reporting marts, and mart tests. |
| `fdp/macros/generate_schema_name.sql` | Controls dbt's target schema naming. |
| `fdp/scripts/enrich_reviews.py` | Earlier OpenAI-based review enrichment script. |
| `ai/ollama_utils.py` | Shared Ollama client and embedding helper. |
| `ai/enrich_reviews.py` | Ollama-based review enrichment used by the Airflow DAG. |
| `ai/rag_chat.py` | Streamlit retrieval-augmented review Q&A app. |
| `ai/text_to_sql.py` | Streamlit natural-language-to-SQL analytics app. |
| `mart_review_insights.sql` | Review-insights SQL draft at the repository root; it is currently outside dbt's configured model path. |
| `dbs/` | Local data files and screenshots. CSVs are excluded by `.gitignore`. |

Generated dbt files (`fdp/target/`, logs, Python bytecode, local environment files, and the review-embedding cache) are runtime artifacts rather than source code.

## Data model

### Raw source tables

The dbt source declaration expects these tables in `ZOMATO.RAW`:

`RESTAURANTS`, `USERS`, `FOOD`, `MENU`, `ORDERS`, `ORDER_ITEMS`, and `REVIEWS`.

The Airflow DAG also copies each corresponding folder from the Snowflake stage. The relevant files must already be staged in `restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, and `reviews/` paths.

### Staging views

| Model | Behavior |
| --- | --- |
| `stg_food` | Selects food ID and name, normalizes the vegetarian classification with `INITCAP`, and excludes null food IDs. |
| `stg_menu` | Converts restaurant IDs and prices to numeric types, keeps rows with a valid restaurant ID and positive price, and selects menu, food, and cuisine fields. |
| `stg_order_items` | Selects order-item identifiers, restaurant/food IDs, price, quantity, and line amount with numeric casts. |
| `stg_orders` | Renames user ID to customer ID, derives city from the final comma-separated part of the restaurant-city field, carries order and payment measures, and derives `is_delivered`. |
| `stg_restaurants` | Renames the source restaurant fields to standard IDs, names, rating, license, link, address, and menu columns. It currently does not normalize the raw rating or cost strings. |
| `stg_reviews` | Casts review fields, joins restaurant data to attach city, and excludes reviews with no comment. |
| `stg_users` | Renames user fields, lowercases email, casts customer ID, age, and family size, and excludes rows with invalid customer IDs. |

Staging tests check uniqueness and non-nullness for selected IDs, plus non-null customer and restaurant keys on orders.

### Analytics marts

| Model | Materialization | Behavior |
| --- | --- | --- |
| `dim_customer` | Table | Customer attributes and age bands (`genZ`, `millenial`, `genx`, `boomer`, or `unknown`). |
| `dim_date` | Table | Calendar dates from 2024-01-01 through 2026-12-31 with year, month, month name, weekday name, and weekend flag. |
| `dim_food` | Table | Food ID, name, and vegetarian classification. |
| `dim_restaurants` | Table | Restaurant ID, name, city, rating, rating count, and cost as `cost_for_two`. |
| `fct_orders` | Incremental merge | Order-level facts, incrementally selected by the latest `order_timestamp`. |
| `fact_order_items` | Incremental merge | Order-item facts joined to order timestamp, date, and city; incrementally selected by latest `order_ts`. |
| `mart_daily_city_revenune` | Table | Daily city order totals, delivered orders, cancellation rate, delivered GMV, and average order value. The filename uses the existing `revenune` spelling. |
| `mart_delivery_sla` | Table | Delivered-order counts and median / 90th-percentile delivery time by city and order hour. |
| `mart_restaurant_performance` | Table | Order count, delivered revenue, average customer rating, and average delivery minutes by restaurant. |

The `fct_orders` tests check unique/non-null order IDs, customer relationships to `dim_customer`, and accepted order statuses (`Delivered`, `Cancelled`, `Refunded`).

`mart_review_insights.sql` groups enriched reviews by review city, topic, and sentiment, reporting review count, mean sentiment score, mean star rating, and flagged key issues. Since it lives at the repository root rather than under `fdp/models/`, it is not currently picked up by `dbt build`.

## Airflow workflow

The `zomato_batch` DAG is scheduled daily (`@daily`) with catchup disabled. It runs these tasks in order:

1. **`reload_raw`** — executes `COPY INTO` for the seven raw tables from `@ZOMATO.RAW.ZOMATO_RAW_STAGE`. Most copies use `ON_ERROR='CONTINUE'`; orders and order items use the default error behavior.
2. **`dbt_build_core`** — runs `dbt build` from `/opt/airflow/dbt/fdp`, using the project's profiles directory and excluding models tagged `ai`.
3. **`enrich_reviews`** — runs `/opt/airflow/ai/enrich_reviews.py` in the AI virtual environment.

The compose file also defines two web apps: `rag-chat` on port `8501` and `text-to-sql` on port `8502`, plus the Airflow UI/API on port `8080`. PostgreSQL stores Airflow metadata.

## AI and application functions

### Shared Ollama helper — `ai/ollama_utils.py`

- **`embed_texts(texts)`** — converts nulls to empty strings, sends text to the configured Ollama embedding model in batches of 32, and returns the embedding vectors.

The module loads `.env` values and creates a shared Ollama client. Defaults are `http://localhost:11434`, `llama3.2:latest`, and `nomic-embed-text` for host, chat model, and embedding model.

### Review search and Q&A — `ai/rag_chat.py`

- **`read_reviews_from_snowflake()`** — connects to Snowflake, samples up to 500 rows from `ZOMATO.STAGING.STG_REVIEWS`, selects review ID, city, rating, and comment, normalizes returned column names to lowercase, then closes the connection.
- **`embed(texts)`** — delegates text vectorization to `embed_texts` in the shared helper.
- **`load_reviews()`** — Streamlit-cached loader; reads `review_embeddings.parquet` if present, otherwise retrieves reviews, embeds their comments, and saves the dataframe to that Parquet cache.
- **`consine_simiarity(vec_a, vec_b)`** — computes cosine similarity for two vectors. The function name is spelled as it appears in the code.
- **`find_similar_reviews(question, df)`** — embeds the question, scores every cached review, and returns the five highest-scoring rows.
- **`ask_llm(question, top_reviews)`** — builds context from the selected reviews and asks the Ollama chat model to answer using those reviews only.

The Streamlit interface accepts a question, displays the generated answer, and expands to show the reviews used. The cache is reused until removed; it is not automatically refreshed when new reviews arrive.

### Natural-language analytics — `ai/text_to_sql.py`

- **`get_connection()`** — cached Snowflake connection using the configured credentials and `MARTS` schema.
- **`generate_sql(question)`** — asks Ollama for a JSON response containing one SQL query and strips selected database/schema prefixes.
- **`is_safe(sql)`** — allows statements beginning with `SELECT` or `WITH` and rejects queries containing a basic list of write/DDL keywords.
- **`run_query(sql)`** — executes the generated SQL and returns Snowflake results as a pandas dataframe.

The interface shows the generated SQL, result count, dataframe, and a bar chart when the result has two columns and the second is numeric. The schema description and keyword filter are simple application checks, not a comprehensive SQL authorization boundary; use a read-only Snowflake role and keep the table description aligned with the actual marts.

### Ollama review enrichment — `ai/enrich_reviews.py`

- **`get_connection()`** — opens a Snowflake connection from environment configuration.
- **`create_output_table(cursor)`** — creates the `ZOMATO.AI` schema and `REVIEW_ENRICHED` table if they do not exist.
- **`get_reviews_to_enrich(cursor)`** — selects up to five raw reviews not already present in the enriched table.
- **`classify_review(comment)`** — calls Ollama and parses JSON with sentiment label, score, topic, and key issue.
- **`save_results(cursor, results)`** — bulk-inserts successful classifications with model name.
- **`main()`** — creates the destination, loads unprocessed reviews, classifies them one by one, persists successful results, closes the connection, and raises an error if all or some classifications fail.

The current topics are food quality, delivery, pricing, service, packaging, and other. Sentiment scores are prompted to fall between -1 and 1. The batch size is hard-coded to five in this script.

### Earlier OpenAI enrichment — `fdp/scripts/enrich_reviews.py`

This separate script contains similarly named functions (`get_connection`, `create_output_table`, `get_reviews_to_enrich`, `classify_review`, `save_results`, and `main`) but calls OpenAI's `gpt-4o-mini` rather than Ollama. The Airflow DAG invokes `ai/enrich_reviews.py`, not this script. The OpenAI version calls `load_dotenv()` and expects `OPENAI_API_KEY`; the provided Docker image does not install the OpenAI package, so this script is not part of the default compose workflow.

## Configuration

Do not commit credentials. Create local environment files from the examples/placeholders and supply valid values through your local secret manager or environment. At minimum, the applications and dbt profile use:

| Variable | Used by |
| --- | --- |
| `SNOWFLAKE_ACCOUNT` | dbt, Airflow Snowflake connection, and Python applications. |
| `SNOWFLAKE_USER` | Snowflake login. |
| `SNOWFLAKE_PASSWORD` | Snowflake login. |
| `SNOWFLAKE_WAREHOUSE` | Python Snowflake connections; the compose Airflow connection currently specifies `ZOMATO_WH`. |
| `SNOWFLAKE_DATABASE` | Python Snowflake connections; the dbt profile currently specifies `ZOMATO`. |
| `SNOWFLAKE_SCHEMA` | Python Snowflake connection default schema. |
| `OLLAMA_HOST` | Ollama server URL; defaults to localhost in Python and `host.docker.internal` in compose. |
| `OLLAMA_MODEL` | Ollama chat model. |
| `OLLAMA_EMBEDDING_MODEL` | Ollama embedding model. |
| `SAMPLE_N` | Present in the compose environment, but the active Ollama enrichment script currently uses a hard-coded batch size of five. |
| `OPENAI_API_KEY` | Only the earlier OpenAI script uses this. |

`fdp/profiles.yml` reads Snowflake account, user, and password from environment variables and uses role `DBT_ROLE`, warehouse `ZOMATO_WH`, and database `ZOMATO`. Configure a Snowflake role with only the permissions the workflow needs. Airflow also expects a connection named `snowflake_default`, supplied in compose through `AIRFLOW_CONN_SNOWFLAKE_DEFAULT`.

## Running locally

### Run dbt

Install dbt with its Snowflake adapter in your Python environment, export the Snowflake variables, then run:

```bash
cd fdp
dbt debug --profiles-dir .
dbt build --profiles-dir .
```

The dbt profile file is in the project directory. `dbt build` runs models and configured tests. The Airflow DAG excludes the `ai` tag from its dbt build; the root-level review insights SQL is not included in the dbt model path at present.

### Run the Streamlit apps directly

Install the needed Python packages (`streamlit`, `ollama`, `snowflake-connector-python`, `pandas`, `numpy`, and `pyarrow`), configure the environment variables, and make sure the Ollama server is running with both configured models available. From the `ai/` directory:

```bash
streamlit run rag_chat.py
streamlit run text_to_sql.py
```

### Run Airflow and the apps with Docker Compose

Create `airflow/.env` with the Snowflake account, user, and password, and any Ollama overrides. Ensure the pre-existing Snowflake stage/tables and Snowflake Airflow connection permissions are ready. Then:

```bash
cd airflow
docker compose build
docker compose up -d
```

Open Airflow at `http://localhost:8080`, the review app at `http://localhost:8501`, and the text-to-SQL app at `http://localhost:8502`. The compose file creates a local `admin` Airflow account with a fixed development password and uses development defaults; replace those settings before exposing the services beyond a trusted local environment.

The compose file mounts the project folders and starts applications, but Ollama itself is not a compose service. When using the defaults, Ollama must be reachable on the host at `http://host.docker.internal:11434`.

## Current implementation notes

- Snowflake, S3, and Airflow infrastructure are assumed to exist in advance; no Terraform, AWS CLI loader, or integration provisioning is included.
- Raw CSV files in `dbs/` are large and ignored by Git. The repository does not include a data-loading script that uploads these local CSVs to S3.
- The active enrichment flow uses Ollama. The OpenAI implementation under `fdp/scripts/` is an alternate/older path.
- `text_to_sql.py` contains a hard-coded schema prompt whose table/column names do not fully match the dbt models (for example singular/plural and some dimensions). Update it to match the deployed Snowflake schema before relying on generated SQL.
- The review-insights SQL is at repository root, outside dbt's `models/` directory, so the DAG's `dbt build` does not materialize it.
- The RAG app's Parquet cache is static once created; delete `ai/review_embeddings.parquet` to force a fresh sample and embeddings.
- The repository contains generated logs, local configuration, and an AWS/Snowflake access artifact. Check the Git index and remove/rotate any credentials or access identifiers that were ever committed. Do not copy secrets into documentation or commit them in future changes.

## License

No license file is currently included. Add a `LICENSE` file before granting reuse rights.
