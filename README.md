# 🌦️ ETL - Impact des conditions météo sur la consommation électrique en France

## 🎯 Objectif
Ce projet vise à analyser la **corrélation entre les conditions météorologiques et la consommation électrique régionale** en France à l’aide d’un pipeline **ETL/ELT scalable** construit sur **Databricks (PySpark)**, orchestré par **Airflow**, et stocké dans **BigQuery**.

---

## 🏗️ Architecture du pipeline

Le pipeline est composé de quatre grandes étapes :

1. **Extract** :
   - Données météo depuis OpenWeatherMap API
   - Données de consommation électrique depuis RTE Open Data
   - Stockage en **format brut (raw)** sur Databricks

2. **Transform** :
   - Nettoyage des valeurs manquantes / doublons
   - Filtrage des outliers
   - Jointure météo ↔ consommation
   - Calcul du KPI : consommation moyenne par température

3. **Load** :
   - Chargement des données transformées dans **BigQuery**

4. **Orchestration (Airflow)** :
   - Exécution quotidienne automatisée du pipeline

---

## ⚙️ Technologies utilisées

| Technologie | Rôle |
|--------------|------|
| **Databricks / PySpark** | Traitement et transformation des données |
| **OpenWeatherMap API** | Extraction des données météo |
| **RTE Open Data API** | Extraction de la consommation électrique |
| **BigQuery (GCP)** | Stockage analytique |
| **Airflow** | Orchestration du pipeline |
| **Metabase** | Visualisation des KPI |

---

## 📂 Structure du projet

etl_meteo_elec/
├── notebooks/ → Notebooks Databricks pour chaque étape
├── airflow_dag/ → DAG Airflow pour automatiser les jobs
├── scripts/ → Scripts Python réutilisables
├── data_samples/ → Exemples de données brutes
├── architecture/ → Diagramme du pipeline
└── requirements.txt → Dépendances du projet

## 🚀 Exécution

### 🔹 Étape 1 — Lancer les notebooks Databricks :
1. `1_extract_api_meteo.ipynb`
2. `2_extract_api_rte.ipynb`
3. `3_transform_spark_elec.ipynb`
4. `4_transform_meteo.ipynb`
5. `5_join_kpi.ipynb`

### 🔹 Étape 2 — Orchestration (Airflow)
Déployer le DAG :
```bash
airflow dags list
airflow dags trigger etl_meteo_elec