# 📋 PROJET — PIPELINE BATCH DE DONNÉES DE SANTÉ

## 🏥 Diabetes Readmission Pipeline — Airflow, Polars, DuckDB et dbt

| | |
|:---|:---|
| **📂 Repo** | `DEHUB/01-diabetes-readmission-pipeline/` |
| **🎯 Objectif** | Construire un pipeline batch reproductible pour analyser les réadmissions hospitalières de patients atteints de diabète |
| **🧰 Technologies principales** | Apache Airflow, Polars, DuckDB et dbt |
| **📥 Source de données** | *Diabetes 130-US Hospitals for Years 1999–2008*, du dépôt UCI |
| **📊 Sujet d’analyse** | Réadmission à l’hôpital dans les 30 jours suivant une sortie |

## 💡 Pitch recruteur

> « J’ai conçu un pipeline batch à partir d’un jeu de données hospitalières publié par UCI. Airflow orchestre le traitement, Polars nettoie les données, DuckDB les stocke localement et dbt construit des modèles analytiques testés. Le projet étudie les réadmissions après une hospitalisation de patients diabétiques, sans prétendre produire un outil de décision médicale. »

## 📥 Données à utiliser

Je remplace Synthea par le jeu **Diabetes 130-US Hospitals for Years 1999–2008**, disponible sur le dépôt UCI. Il rassemble des dossiers de soins concernant des patients diabétiques dans 130 hôpitaux américains sur la période 1999–2008. Une ligne correspond à une hospitalisation et les variables incluent notamment la durée du séjour, les diagnostics, les médicaments et les réadmissions. ([archive.ics.uci.edu](https://archive.ics.uci.edu/datasets?search=130&utm_source=openai))

Le jeu de données est publié sous licence **CC BY 4.0** : indique la source et l’attribution dans le README si tu le réutilises ou le modifies. ([archive.ics.uci.edu](https://archive.ics.uci.edu/dataset/296/diabetes%2B130-u?utm_source=openai))

**Source officielle :** UCI Machine Learning Repository, *Diabetes 130-US Hospitals for Years 1999–2008*.

### Sujet d’analyse

La variable de réadmission permet de distinguer, selon les catégories du jeu, les patients réadmis dans les 30 jours, plus tard ou non réadmis. Le projet peut produire des statistiques descriptives sur ces groupes.

> Ce jeu de données contient des dossiers hospitaliers publiés et dé-identifiés, mais il ne faut pas le présenter comme une source synthétique. Les résultats sont historiques et ne décrivent pas nécessairement les pratiques médicales actuelles.

## 🧭 Architecture

```text
Jeu de données UCI
        ↓
Airflow — orchestration du pipeline
        ↓
Polars — validation et préparation des fichiers
        ↓
Parquet — données préparées
        ↓
DuckDB — stockage analytique local
        ↓
dbt — modèles SQL, tests et documentation
        ↓
Tables analytiques — réadmissions et séjours
```

- **Airflow** orchestre les tâches et permet de suivre leur exécution.
- **Polars** lit les fichiers, vérifie les types et prépare les données.
- **DuckDB** stocke les données et exécute les requêtes localement.
- **dbt** organise les transformations en modèles documentés et testés.

## 🔄 Étapes du pipeline

1. Télécharger le jeu de données depuis UCI et noter sa version et sa licence.
2. Contrôler les fichiers, en-têtes et valeurs de la variable de réadmission.
3. Lire les fichiers avec Polars et vérifier les types, valeurs manquantes et doublons.
4. Préparer les données, puis les écrire en Parquet.
5. Charger les données préparées dans DuckDB.
6. Exécuter les modèles dbt de staging et d’analyse.
7. Lancer les tests dbt et enregistrer les résultats.
8. Consulter les journaux Airflow, les volumes traités et les éventuelles erreurs.

## 🧭 DAG Airflow proposé

```text
check_source_files
        ↓
validate_schema
        ↓
prepare_with_polars
        ↓
load_into_duckdb
        ↓
run_dbt_build
        ↓
generate_report
```

Le pipeline doit détecter explicitement une modification de schéma ou des catégories inattendues dans la colonne de réadmission, plutôt que de les interpréter silencieusement.

## 📊 Modèles dbt à créer

### Modèles de staging

- `stg_encounters` : noms de colonnes normalisés et types harmonisés.
- `stg_admissions` : informations d’admission, durée de séjour et nombre de visites antérieures, selon les colonnes disponibles.
- `stg_readmissions` : variable de réadmission nettoyée et valeurs manquantes documentées.

### Modèles analytiques

- `int_encounter_features` : variables descriptives préparées au niveau d’une hospitalisation.
- `mart_readmissions_by_group` : volumes et taux de réadmission par groupe analysé.
- `mart_length_of_stay` : durée de séjour agrégée selon les catégories de réadmission.

Exemples d’analyses :

- part des hospitalisations suivies d’une réadmission à 30 jours ;
- durée moyenne du séjour selon la catégorie de réadmission ;
- nombre de réadmissions par âge, si cette variable est exploitable ;
- répartition des réadmissions selon le nombre de visites antérieures.

## 🧪 Qualité des données

Ajoute des contrôles pour vérifier que :

- les identifiants nécessaires ne sont pas nuls ;
- les catégories de réadmission correspondent aux valeurs documentées ;
- les durées de séjour et nombres de visites respectent les règles définies ;
- les doublons sont détectés et quantifiés ;
- les valeurs manquantes sont mesurées avant et après préparation ;
- le nombre de lignes source, préparées et chargées est suivi.

Si plusieurs lignes concernent un même patient, documente le grain de chaque modèle : **une ligne par hospitalisation** n’est pas équivalente à **une ligne par patient**.

## 📁 Organisation du dépôt

```text
01-diabetes-readmission-pipeline/
├── README.md
├── docker-compose.yml
├── .gitignore
├── .env.example
├── dags/
│   └── diabetes_readmission_pipeline.py
├── src/
│   ├── validate_source.py
│   ├── prepare_data.py
│   └── load_duckdb.py
├── data/
│   ├── raw/
│   └── processed/
├── warehouse/
│   └── diabetes.duckdb
├── dbt_health/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
├── tests/
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    └── source_and_license.md
```

## ✅ Livrables portfolio

- Dépôt Git avec le DAG Airflow, le code Polars et les modèles dbt.
- README décrivant la source, sa licence, les étapes du pipeline et les limites d’interprétation.
- Diagramme d’architecture.
- Tests de qualité et rapport sur les volumes et valeurs manquantes.
- Exemples de requêtes DuckDB et résultats agrégés.
- Dictionnaire des données précisant le grain des tables.

## 📌 Positionnement

Ce projet démontre un pipeline **batch local** complet, de l’ingestion à la production de tables analytiques. Il met en pratique Airflow, Polars, DuckDB et dbt sur un jeu de données hospitalières réel, plutôt que sur des dossiers synthétiques.

Les analyses restent **descriptives** : elles ne prouvent pas qu’une variable cause une réadmission et ne doivent pas être utilisées pour prendre des décisions concernant des patients.
