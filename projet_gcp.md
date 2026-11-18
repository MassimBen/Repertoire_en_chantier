# VERSION COMPLÈTE

Les deux projets ci-dessous sont séparés par **batch** et **streaming**, comme les cours Google Cloud prévus dans ton programme. Le parcours Data Engineer répertorie notamment *Building Batch Data Pipelines on Google Cloud*, *Building Resilient Streaming Analytics Systems on Google Cloud* et *Serverless Data Processing with Dataflow*. ([cloud.google.com](https://cloud.google.com/learn/training/data-engineering-and-analytics?authuser=117&utm_source=openai))

**À noter :** Dataproc, Dataflow, BigQuery et GCS sont les services Google Cloud au cœur des projets. **Kestra et dbt Fusion sont des extensions personnelles** : utiles pour ton portfolio, mais à ne pas présenter comme des outils enseignés par les cours eux-mêmes.

---

# 📋 PROJET 1 — PIPELINE BATCH

## 🏥 Synthea Batch Pipeline — Dataproc, PySpark, GCS et BigQuery

| | |
|:---|:---|
| **📂 Repo** | `DEHUB/01-synthea-gcp-batch-pipeline/` |
| **☁️ Projet GCP** | `note` — ton projet Google Cloud principal |
| **🎯 Objectif** | Construire un pipeline batch reproductible de traitement et d’analyse de données synthétiques |
| **🧰 Technologies principales** | Google Cloud Storage, Dataproc Serverless, PySpark et BigQuery |
| **🧰 Extensions portfolio** | Kestra pour l’orchestration ; dbt Fusion exécuté localement pour les modèles SQL et les tests |

## 💡 Pitch recruteur

> « J’ai construit un pipeline batch à partir des données synthétiques Synthea. Les fichiers bruts sont stockés dans Cloud Storage, puis traités avec PySpark sur Dataproc Serverless. Les données préparées sont chargées dans BigQuery pour les analyses. J’ai ajouté Kestra pour orchestrer les étapes et dbt Fusion, exécuté localement, pour créer et tester les modèles analytiques. »

## 🎓 Lien avec le parcours Data Engineer

Ce projet met principalement en pratique les thèmes du cours **Building Batch Data Pipelines on Google Cloud** : choix d’un schéma batch, traitement avec Spark sur Dataproc et utilisation de BigQuery. Le parcours Google Cloud mentionne Spark sur Dataproc parmi les technologies abordées dans ce cours. ([cloud.google.com](https://cloud.google.com/blog/topics/training-certifications/google-clouds-data-engineering-learning-path-is-now-updated?utm_source=openai))

Les cours d’introduction et de modernisation des data lakes et data warehouses apportent le contexte pour concevoir les zones de stockage GCS et l’entrepôt BigQuery. Le lab **Build a Data Warehouse with BigQuery** peut compléter la partie modélisation et analyse. ([cloud.google.com](https://cloud.google.com/learn/training/data-engineering-and-analytics?authuser=117&utm_source=openai))

**Kestra et dbt Fusion ne sont pas requis pour démontrer ces objectifs de cours.** Tu peux les garder comme extensions et indiquer clairement leur rôle.

## 🧭 Architecture

```text
Fichiers CSV Synthea
        ↓
Cloud Storage — zone raw
        ↓
Dataproc Serverless — jobs PySpark batch
        ↓
Cloud Storage — données préparées en Parquet
        ↓
BigQuery — tables de staging
        ↓
dbt Fusion local — modèles SQL et tests (extension)
        ↓
BigQuery — tables analytiques
```

Kestra peut déclencher et suivre le traitement, mais son ajout ne remplace pas les services Google Cloud étudiés.

## 📥 Données à récupérer

Synthea produit des dossiers médicaux **synthétiques**, et non des dossiers de vrais patients. Pour une première version, récupère un jeu d’exemple CSV sur le site officiel ou génère un jeu toi-même avec le projet Synthea. Vérifie les fichiers et leurs colonnes avant de définir les schémas BigQuery.

**Sources officielles :**

- [Téléchargements Synthea](https://synthetichealth.github.io/downloads.html)
- [Projet Synthea sur GitHub](https://github.com/synthetichealth/synthea)

Commence avec ces trois fichiers :

| Fichier | Utilisation |
|---|---|
| `patients.csv` | Patients et analyses démographiques |
| `encounters.csv` | Rencontres médicales et analyses par période |
| `conditions.csv` | Conditions médicales et analyses par groupe démographique |

Les fichiers disponibles et leurs colonnes peuvent varier selon le jeu et la configuration. Contrôle les en-têtes des CSV téléchargés avant de construire le pipeline.

## 🔄 Étapes du pipeline

1. Télécharger ou générer les fichiers CSV Synthea.
2. Vérifier la présence des fichiers, leurs en-têtes et les colonnes attendues.
3. Déposer les fichiers bruts dans un bucket GCS.
4. Lancer les jobs PySpark sur Dataproc Serverless.
5. Contrôler les types, les valeurs manquantes, les doublons et les règles de qualité.
6. Écrire les fichiers préparés en Parquet dans une zone GCS `curated/`.
7. Charger les données préparées dans les tables de staging BigQuery.
8. Exécuter **localement**, si tu conserves cette extension, dbt Fusion pour créer les modèles et lancer les tests.
9. Consulter les résultats, les journaux et les volumes traités.

## 📊 Analyses à présenter

- Nombre de patients par tranche d’âge ou région, si les champs le permettent.
- Nombre de rencontres médicales par période.
- Principales conditions médicales par groupe démographique.
- Nombre de doublons et de champs manquants.
- Volumes avant et après nettoyage.
- Lignes traitées ou rejetées et erreurs rencontrées.

Les résultats sont des analyses de **données synthétiques** : ils ne décrivent pas une population réelle.

## 🧪 Qualité et sécurité

- Vérifier les colonnes obligatoires et leurs types.
- Tester l’unicité des clés lorsque le grain des données s’y prête.
- Comparer les volumes source et les volumes traités.
- Documenter les lignes rejetées plutôt que de les supprimer sans suivi.
- Ne jamais versionner de clé de compte de service ni de secret.
- Donner aux identités utilisées uniquement les permissions GCP nécessaires.

## 📁 Organisation du dépôt

```text
01-synthea-gcp-batch-pipeline/
├── README.md
├── docker-compose.yml               # Kestra local, si conservé
├── .gitignore
├── .env.example
├── kestra/
│   └── flows/
├── spark/
│   └── jobs/
│       ├── clean_patients.py
│       ├── prepare_encounters.py
│       └── prepare_conditions.py
├── dbt/                              # extension : dbt Fusion local
│   ├── dbt_project.yml
│   ├── models/
│   └── tests/
├── tests/
└── docs/
    ├── architecture.md
    └── data_dictionary.md
```

## ✅ Livrables portfolio

- Dépôt Git avec les jobs PySpark, les configurations et, si utilisé, les flows Kestra.
- README expliquant l’architecture, les données Synthea, les étapes et les choix techniques.
- Schéma du pipeline.
- Résultats de contrôles de qualité.
- Exemples de requêtes BigQuery ou petit tableau de bord.
- Captures Kestra si tu utilises Kestra.
- Modèles et résultats de tests dbt si tu utilises dbt Fusion.

## 📌 Positionnement

Ce projet démontre surtout tes compétences **batch** : données en fichiers, stockage GCS, traitement Spark sur Dataproc et analyse dans BigQuery. Garde Kestra et dbt Fusion comme des extensions clairement identifiées, et non comme des prérequis du cours.

---

# 📋 PROJET 2 — PIPELINE STREAMING

## 📡 Synthetic Health Events Pipeline — Pub/Sub, Dataflow et BigQuery

| | |
|:---|:---|
| **📂 Repo** | `DEHUB/02-synthea-gcp-streaming-pipeline/` |
| **☁️ Projet GCP** | `note` — ton projet Google Cloud principal |
| **🎯 Objectif** | Construire un pipeline de démonstration qui traite des événements synthétiques au fil de leur arrivée |
| **🧰 Technologies principales** | Pub/Sub, Dataflow, Apache Beam et BigQuery |
| **🧰 Extension portfolio** | dbt Fusion local, facultatif, pour des modèles analytiques supplémentaires |

## 💡 Pitch recruteur

> « J’ai développé un pipeline streaming de démonstration. Un générateur produit des événements synthétiques, Pub/Sub les transporte, et Dataflow les valide, les transforme et les écrit dans BigQuery. J’ai ajouté des contrôles sur les événements invalides, les doublons et les volumes afin d’illustrer le suivi et la fiabilité du pipeline. »

## 🎓 Lien avec le parcours Data Engineer

Ce projet correspond aux sujets du cours **Building Resilient Streaming Analytics Systems on Google Cloud** : traiter et agréger des flux avec Dataflow, puis stocker les résultats dans BigQuery pour l’analyse. Le cours officiel décrit ce type de chaîne streaming. ([cloud.google.com](https://cloud.google.com/blog/topics/training-certifications/google-clouds-data-engineering-learning-path-is-now-updated?utm_source=openai))

Il complète le cours **Serverless Data Processing with Dataflow** et le projet batch en mettant l’accent sur des événements traités à mesure qu’ils arrivent. Le parcours Data Engineer liste ces cours dans son offre actuelle. ([cloud.google.com](https://cloud.google.com/learn/training/data-engineering-and-analytics?authuser=117&utm_source=openai))

## 🧭 Architecture

```text
Générateur d’événements synthétiques
        ↓
Pub/Sub — réception des messages
        ↓
Dataflow / Apache Beam — validation et transformation
        ↓
 ┌─────────────────────┴────────────────────┐
 ↓                                          ↓
BigQuery — événements valides       Sortie de rejet / erreurs
        ↓
Requêtes et tableaux de bord
```

Le générateur est une source de test : **ce n’est pas une source de données de santé réelles**.

## 📨 Événements à générer

Définis un schéma JSON simple et documenté, par exemple :

- identifiant d’événement ;
- type d’événement ;
- identifiant patient synthétique ;
- horodatage de création ;
- attributs propres au type d’événement.

Ne publie pas d’informations personnelles réelles. Les identifiants et valeurs doivent être générés pour la démonstration.

## 🔄 Étapes du pipeline

1. Définir le schéma des événements et les règles de validation.
2. Générer des événements synthétiques valides et invalides.
3. Publier les messages valides et invalides dans Pub/Sub.
4. Traiter le flux avec Dataflow et Apache Beam.
5. Valider les champs obligatoires, les types et les horodatages.
6. Écrire les événements valides dans BigQuery.
7. Séparer ou signaler les messages invalides pour les inspecter.
8. Comparer les volumes publiés, traités, rejetés et écrits.
9. Tester les doublons, les erreurs et le redémarrage du traitement.

## 📊 Analyses à présenter

- Nombre d’événements par type et par période.
- Volumes reçus, traités et rejetés.
- Répartition des erreurs de validation.
- Doublons détectés.
- Délai entre émission et traitement, si les horodatages le permettent.
- Indicateurs d’exécution et coûts de la démonstration.

## 🧪 Qualité et fiabilité

- Vérifier que les champs obligatoires sont présents.
- Contrôler le format et la validité des horodatages.
- Documenter le traitement des messages invalides.
- Tester des événements dupliqués et des événements arrivant en retard.
- Comparer le nombre de messages publiés et les sorties BigQuery.
- Documenter les limites, la supervision et les hypothèses de coût.

## 📁 Organisation du dépôt

```text
02-synthea-gcp-streaming-pipeline/
├── README.md
├── .gitignore
├── .env.example
├── generator/
│   └── events/
├── dataflow/
│   ├── pipeline/
│   └── tests/
├── dbt/                              # extension facultative
│   ├── dbt_project.yml
│   ├── models/
│   └── tests/
├── docs/
│   ├── architecture.md
│   ├── event_schema.md
│   └── reliability_and_costs.md
└── tests/
```

## ✅ Livrables portfolio

- Dépôt Git avec le générateur, le pipeline Dataflow et les tests.
- README avec le schéma d’événement, les étapes de déploiement et les choix d’architecture.
- Diagramme Pub/Sub → Dataflow → BigQuery.
- Exemples de requêtes BigQuery et résultats de contrôles.
- Description du traitement des erreurs, des doublons et des reprises.
- Captures ou journaux de démonstration, sans secrets ni données sensibles.

## 📌 Positionnement

Ce projet démontre le volet **streaming** du parcours : Pub/Sub pour l’ingestion, Dataflow pour le traitement et BigQuery pour l’analyse. Le projet 1 couvre le batch avec Dataproc et PySpark ; les deux projets sont donc complémentaires plutôt que redondants. ([cloud.google.com](https://cloud.google.com/learn/training/data-engineering-and-analytics?authuser=117&utm_source=openai))
