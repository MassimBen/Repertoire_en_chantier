Voici le projet mis à jour avec dbt initialisé dans un dossier et connecté directement à BigQuery, sans asset Python d'orchestration.

---

# 📋 PROJET PRIORITAIRE POST-ZOOMCAMP

## 🏥 Cnam Open Data Pipeline — ELT avec Bruin, dbt et BigQuery

| | |
|:---|:---|
| **📂 Repo** | `DEHUB/01-cnam-health-pipeline/` |
| **☁️ Projet GCP** | `note` (ton Google Cloud Project principal) |
| **🎯 Objectif** | Pipeline ELT reproductible sur données publiques CNAM, avec documentation des particularités et limites de chaque source |

---

## 💡 Pitch recruteur

> « J'ai construit un pipeline ELT reproductible sur des jeux de données publics de l'Assurance Maladie. Bruin gère l'ingestion et l'orchestration ; dbt transforme les données dans BigQuery, construit les modèles analytiques et exécute les tests. J'ai documenté les millésimes, les codes géographiques et les règles de confidentialité propres aux sources — la comorbidité, les professionnels de santé et les dépenses de soins via API — puis exposé les indicateurs dans un dashboard Looker Studio. »

---

## 🧭 Architecture déployée sur GCP (`note`)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        BRUIN (orchestration)                             │
│                    ┌─────────────────┐                                    │
│                    │   .bruin.yml    │  ← hors dépôt (secrets GCP)       │
│                    └─────────────────┘                                    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
    ┌───────────────────────────────┼───────────────────────────────┐
    ▼                               ▼                               ▼
┌─────────────┐              ┌─────────────┐              ┌─────────────────┐
│  Source 1   │              │  Source 2   │              │   Source 3      │
│ API JSON    │              │   CSV       │              │    CSV          │
│ data.ameli  │              │ data.gouv   │              │  data.gouv      │
│ comorbidité │              │ professionnels│            │  dépenses       │
└──────┬──────┘              └──────┬──────┘              └───────┬─────────┘
       │                            │                             │
       └────────────────────────────┼─────────────────────────────┘
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    BIGQUERY — dataset `cnam_raw`                         │
│  cnam_raw.comorbidites  │  cnam_raw.professionnels  │  cnam_raw.depenses │
│  (chargé via API)       │  (chargé via CSV)        │  (chargé via CSV)  │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         dbt (transformation)                             │
│                    dbt init dans dossier cnam_dbt/                       │
│                    profiles.yml → connexion BigQuery `note`              │
│                                                                          │
│   staging/           intermediate/            marts/                    │
│   ├── stg_comorbidites.sql    ├── int_territoires_normalises.sql   ├── dim_*        │
│   ├── stg_professionnels.sql  ├── int_mesures_qualifiees.sql       └── fact_*       │
│   └── stg_depenses.sql                                                               │
│                                                                          │
│   + tests : nullités, unicité, intégrité référentielle, seuils           │
│   + docs auto-générées via dbt docs                                      │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    BIGQUERY — dataset `cnam_mart`                        │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      LOOKER STUDIO (dashboard)                           │
│   Page 1 : Comorbidité    │  Page 2 : Professionnels  │  Page 3 : Dépenses │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🔗 Sources de données confirmées

| # | Famille | Source | URL | Type | Accès |
|---|---------|--------|-----|------|-------|
| 1 | **Comorbidité** | API data.ameli.fr | `https://data.ameli.fr/explore/assets/comorbidites/api/` | JSON | API avec pagination |
| 2 | **Professionnels de santé** | CSV data.gouv.fr | `https://www.data.gouv.fr/datasets/acces-aux-soins-patientele-medecin-traitant-moyenne-des-medecins-generalistes-par-territoire-departement-region-par-annee/` | CSV | Téléchargement direct |
| 3 | **Dépenses de soins** | CSV data.gouv.fr | `https://www.data.gouv.fr/datasets/donnees-nationales-sur-les-depenses-dassurance-maladie-relatives-aux-soins-de-ville` | CSV | Téléchargement direct |

---

## 📊 Périmètre des données actualisé

| Famille | Indicateurs envisagés | Grain | Source | Particularités |
|---------|----------------------|-------|--------|--------------|
| **Comorbidité** | Bénéficiaires, prévalence par pathologie, taux de comorbidité | pathologie × année (agrégat national) | API data.ameli | Pagination obligatoire, champs JSON imbriqués possibles |
| **Professionnels de santé** | Effectifs médecins traitants, patientèle moyenne, densité | territoire (département/région) × année | CSV data.gouv | Corse 2A/2B, DOM, métropole vs hors métropole |
| **Dépenses de soins** | Montants remboursés, nombre de remboursements, bénéficiaires | type de prestation × mode de remboursement × année | CSV data.gouv | Régime général uniquement, hors hospitalisation et ALD , agrégat national |

> **⚠️ Impossibilité de croisement** : Les 3 sources n'ont **pas le même niveau géographique** — seuls les professionnels ont un découpage département/région. La comorbidité et les dépenses sont agrégées nationalement ou sur des zonages différents. Documenter cette limitation.

---

## 🔄 Étapes du pipeline

### 1. Ingestion — Bruin

| Asset | Source | Type Bruin | Détail technique |
|-------|--------|-----------|----------------|
| `comorbidites.asset.yml` | API data.ameli | **Asset Python** | requêtes paginées, gestion du rate limiting, flattening JSON |
| `professionnels.asset.yml` | CSV data.gouv | ingestr ou Python | téléchargement + chargement direct |
| `depenses.asset.yml` | CSV data.gouv | ingestr ou Python | téléchargement + chargement direct |

---

### 2. dbt initialisé dans le repo

**Initialisation** (à faire une fois) :

```
cd 01-cnam-health-pipeline/
dbt init cnam_dbt
# Choix : BigQuery, oauth ou service account, projet note
```

**Configuration connexion** dans `cnam_dbt/profiles.yml` :

```
cnam_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: note
      dataset: cnam_raw
      keyfile: /chemin/vers/cle-service-account.json  # dev local
      threads: 4
      timeout_seconds: 300
    
    prod:
      type: bigquery
      method: service-account-json
      project: note
      dataset: cnam_mart
      # keyfile_json via variable d'environnement DBT_ENV_SECRET_BIGQUERY_KEY
      threads: 4
      timeout_seconds: 300
```

**Fichier example versionné** : `cnam_dbt/profiles.yml.example` (sans secrets)

---

### 3. Modélisation dbt — ajustée aux sources

```
cnam_dbt/
├── models/
│   ├── staging/
│   │   ├── stg_comorbidites.sql
│   │   ├── stg_professionnels.sql
│   │   └── stg_depenses.sql
│   ├── intermediate/
│   │   ├── int_territoires_normalises.sql
│   │   └── int_mesures_qualifiees.sql
│   └── marts/
│       ├── dim_annee.sql
│       ├── dim_pathologie.sql
│       ├── dim_territoire.sql
│       ├── dim_prestation.sql
│       ├── fact_comorbidite.sql
│       ├── fact_densite_professionnels.sql
│       └── fact_depenses_soins.sql
│
├── tests/
│   ├── assert_json_parsing.sql
│   ├── assert_geo_consistency.sql
│   └── assert_suppression_flags.sql
│
├── macros/
│   └── utils/
│       └── flatten_json.sql
│
└── dbt_project.yml
    # name: 'cnam_dbt'
    # models: +materialized: table
    # surcharget dataset par environnement
```

> **Suppression de `dim_specialite.sql`** : La source professionnels ne contient pas de spécialité détaillée, uniquement médecins généralistes traitants.

---

### 4. Règles de données — spécifiques aux sources retenues

| Règle | Application |
|-------|-------------|
| **Confidentialité API** | La comorbidité via API data.ameli a des seuils de non-diffusion. Conserver les valeurs brutes, créer `is_suppressed` si documenté. |
| **Géographie désynchronisée** | `code_geo_source` conservé ; `code_geo_normalise` NULL pour comorbidité/dépenses si non disponible. Documenter. |
| **Champs JSON API** | La comorbidité peut retourner des objets imbriqués. Stocker sous forme de string JSON dans `cnam_raw`, parser en staging. |
| **Rupture série professionnels** | Vérifier si la définition "médecin traitant" change (2016 : instauration du parcours de santé). |

---

### 5. Tests dbt — spécifiques ajoutés

- cohérence géographique : fact_comorbidite sans code_geo_normalisé (source agrégée)
- parsing JSON réussi : champs extraits non NULL quand raw JSON l'est
- pagination complète : comparer lignes ingérées avec total annoncé API
- seuils confidentialité : valeur masquée flaguée, jamais remplacée par zéro

---

### 6. Orchestration — commandes séparées documentées

Pas de script Python run_dbt.py. Séquence manuelle puis CI :

```
# 1. Bruin ingestion
bruin run --environment prod assets/ingestion/

# 2. Vérification données chargées
bq query --project_id note "SELECT COUNT(*) FROM cnam_raw.comorbidites"

# 3. dbt build
cd cnam_dbt/
dbt build --target prod

# 4. Vérification tests
# Résultats dans stdout + BigQuery audit logs
```

Version CI (script shell coordonné) :

```
#!/bin/bash
# run_pipeline.sh — documenté comme orchestrateur manuel/CI, pas Bruin natif
set -e
bruin run --environment prod ../assets/ingestion/
cd ../cnam_dbt && dbt build --target prod
```

---

## ☁️ Configuration GCP (`note`)

### Structure BigQuery

| Dataset | Tables | Purpose |
|---------|--------|---------|
| `note.cnam_raw` | `comorbidites`, `professionnels`, `depenses` | Données brutes post-ingestion |
| `note.cnam_mart` | `dim_*`, `fact_*` | Modèles analytiques |

### Service Account IAM

| Rôle | Utilisation |
|------|-------------|
| `roles/bigquery.dataEditor` | Charger données Bruin |
| `roles/bigquery.jobUser` | Exécuter jobs dbt |
| `roles/bigquery.dataViewer` | Lecture Looker Studio |

### Fichiers secrets (hors dépôt)

| Fichier | Stockage |
|---------|----------|
| `.bruin.yml` | CI/CD secrets ou Secret Manager |
| `cnam_dbt/profiles.yml` | Généré CI via variables d'environnement |
| clé service account JSON | Secret Manager GCP |

### Fichiers example versionnés

| Fichier | Contenu |
|---------|---------|
| `cnam_dbt/profiles.yml.example` | Structure sans keyfile ni clés |
| `.bruin.yml.example` | Structure sans credentials |

---

## 📈 Dashboard Looker Studio

| Page | Titre | Source | Limites affichées |
|------|-------|--------|-------------------|
| 1 | Comorbidité — prévalence et taux | data.ameli API | Échelle nationale, seuils non-diffusi |
| 2 | Professionnels de santé — densité | data.gouv CSV | Département/région, définition médecin traitant post-2016 |
| 3 | Dépenses de soins — évolution | data.gouv CSV | Régime général, hors ALD-hospitalisation, pas de répartition géo |

> **Avertissement obligatoire** : « Les données présentées ne couvrent pas le même périmètre géographique selon les indicateurs. Aucun croisement géographique n'est possible entre les sources. »

---

## 🧪 Mesures à produire

| Mesure | Méthode | Outil GCP |
|--------|---------|-----------|
| Jeux et millésimes intégrés | Inventaire assets exécutés | Bruin logs → Cloud Logging |
| Lignes chargées et rejetées | Logs ingestion + requêtes SQL | BigQuery audit |
| Territoires reconnus | Comptage post-normalisation | BigQuery |
| Valeurs absentes ou masquées | Comptage par source et millésime | BigQuery + documentation |
| Durée Bruin | Timestamps pipeline start/end | Cloud Logging |
| Durée dbt | `dbt build` logs | Cloud Logging |
| Coût BigQuery | Bytes billed par requête | Cloud Billing export → BigQuery |

---

## 📁 Structure repo mise à jour

```
01-cnam-health-pipeline/
├── README.md
├── .gitignore
│
├── .bruin.yml                          ← HORS DÉPÔT
├── .bruin.yml.example
├── pipeline.yml
│
├── assets/
│   ├── ingestion/
│   │   ├── comorbidites.asset.yml
│   │   ├── comorbidites_ingest.py
│   │   ├── professionnels.asset.yml
│   │   └── depenses.asset.yml
│   └── quality/                        ← contrôles post-ingestion optionnels
│       └── check_row_counts.sql
│
├── cnam_dbt/                           ← dbt init ici
│   ├── dbt_project.yml
│   ├── profiles.yml                    ← HORS DÉPÔT (généré CI)
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_comorbidites.sql
│   │   │   ├── stg_professionnels.sql
│   │   │   └── stg_depenses.sql
│   │   ├── intermediate/
│   │   │   ├── int_territoires_normalises.sql
│   │   │   └── int_mesures_qualifiees.sql
│   │   └── marts/
│   │       ├── dim_annee.sql
│   │       ├── dim_pathologie.sql
│   │       ├── dim_territoire.sql
│   │       ├── dim_prestation.sql
│   │       ├── fact_comorbidite.sql
│   │       ├── fact_densite_professionnels.sql
│   │       └── fact_depenses_soins.sql
│   ├── tests/
│   │   ├── assert_json_parsing.sql
│   │   ├── assert_geo_consistency.sql
│   │   └── assert_suppression_flags.sql
│   └── macros/
│       └── utils/
│           └── flatten_json.sql
│
├── scripts/
│   └── run_pipeline.sh                 ← coordonne Bruin + dbt
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── tfvars/
│   │   └── note.tfvars
│   └── outputs.tf
│
├── docs/
│   ├── architecture.png
│   ├── data_dictionary.md
│   ├── sources_limitations.md
│   └── LEARNINGS.md
│
├── data/
│   └── sample/
│
└── .github/
    └── workflows/
        └── ci.yml                      ← auth GCP, génère profiles.yml, run_pipeline.sh
```

---

## ✅ Checklist de livraison

- [ ] Sources vérifiées : 3 URLs accessibles, formats confirmés API/CSV/CSV
- [ ] Asset API testé : pagination complète, pas de perte de données
- [ ] Bruin ingère vers `note.cnam_raw`
- [ ] dbt init puis dbt build réussi sur `note`, tests passent
- [ ] Orchestration documentée : commandes séparées ou script shell coordonné
- [ ] Géographie : incohérences documentées, pas de croisement possible
- [ ] JSON API : parsing flattening testé et documenté
- [ ] Dashboard 3 pages avec avertissements périmètre visibles
- [ ] Coûts mesurés via Cloud Billing export
- [ ] Terraform plan/apply/destroy testé sur projet `note`
- [ ] dbt docs générée et accessible ou versionnée

---

# 🏥 Projet principal — Synthea avec Kestra, Dataproc et dbt Fusion

**Repo :** `DEHUB/01-synthea-kestra-dataproc-dbt/`  
**Période :** octobre–novembre 2026  
**Stack :** Kestra, Dataproc Serverless, PySpark, Google Cloud Storage, BigQuery, dbt Fusion

## 🎯 Objectif

Construire un pipeline de données de santé de bout en bout à partir des données synthétiques Synthea. Kestra orchestre les étapes, Dataproc exécute les traitements PySpark, GCS conserve les fichiers et BigQuery héberge les tables analytiques. dbt Fusion, installé et exécuté localement, construit les modèles SQL dans BigQuery et teste leur qualité.

```text
Données synthétiques Synthea
      ↓
Google Cloud Storage — fichiers bruts
      ↓
Kestra — orchestration
      ↓
Dataproc Serverless — jobs PySpark
      ↓
Google Cloud Storage — données nettoyées en Parquet
      ↓
BigQuery — tables de staging
      ↓
dbt Fusion exécuté en local — modèles SQL et tests
      ↓
BigQuery — tables analytiques
```

## 📥 Récupérer les données Synthea

Synthea est un générateur de dossiers médicaux synthétiques. Pour ce projet, le plus simple est de télécharger un **jeu d’exemple au format CSV**, puis d’envoyer les fichiers dans ton bucket GCS.

- **Page officielle des téléchargements Synthea :** [synthetichealth.github.io/downloads](https://synthetichealth.github.io/downloads.html)
- **Page officielle Synthea et jeux d’exemple :** [synthetichealth.github.io/synthea](https://synthetichealth.github.io/synthea/)
- **Code source et instructions pour générer tes propres données :** [github.com/synthetichealth/synthea](https://github.com/synthetichealth/synthea)

La page des téléchargements propose des jeux d’exemple et des archives de plus grande taille. Pour commencer, prends le jeu **CSV d’exemple** plutôt qu’une grosse archive : il sera plus rapide à téléverser et à traiter pendant le développement. Synthea propose aussi des formats FHIR, mais le CSV est le plus simple pour ce pipeline PySpark–BigQuery. ([synthetichealth.github.io](https://synthetichealth.github.io/synthea/?trk=public_post_comment-text&utm_source=openai))

### Quels fichiers récupérer ?

Pour les analyses prévues, commence avec ces fichiers CSV :

| Fichier | Contenu | Utilisation dans le projet |
|---|---|---|
| `patients.csv` | Identité synthétique, naissance, décès éventuel, sexe, adresse et données démographiques | Dimension patients, répartition par âge ou région |
| `encounters.csv` | Rencontres médicales : dates, type, patient, établissement et coûts selon les champs présents | Faits de rencontres par période |
| `conditions.csv` | Conditions ou problèmes de santé associés aux patients et aux rencontres | Principales conditions par groupe démographique |

Tu peux ensuite ajouter :

| Fichier | Contenu | Utilisation possible |
|---|---|---|
| `observations.csv` | Mesures et résultats observés, par exemple certaines valeurs cliniques | Analyses de mesures et indicateurs cliniques |
| `medications.csv` | Médicaments associés aux patients ou aux rencontres | Analyses de prescriptions |
| `procedures.csv` | Procédures réalisées | Activité médicale |
| `organizations.csv` | Organisations ou établissements | Analyses par établissement |
| `providers.csv` | Professionnels de santé | Analyses par professionnel |

Les exports Synthea contiennent des données cliniques comme les patients, les rencontres, les conditions, les médicaments et les observations. Les fichiers disponibles et leurs champs peuvent dépendre du jeu téléchargé ou de la configuration utilisée pour générer les données. Vérifie donc les en-têtes des CSV avant de figer ton schéma BigQuery. ([github.com](https://github.com/synthetichealth/synthea?utm_source=openai))

**Recommandation pour une première version :** utilise `patients.csv`, `encounters.csv` et `conditions.csv`. Ce trio permet de réaliser les analyses principales sans alourdir inutilement le projet.

Exemple d’organisation locale avant l’envoi vers GCS :

```text
data/
└── synthea/
    ├── patients.csv
    ├── encounters.csv
    ├── conditions.csv
    ├── observations.csv
    └── medications.csv
```

Puis, organise les fichiers dans GCS par étape :

```text
gs://<bucket-synthea>/raw/patients/patients.csv
gs://<bucket-synthea>/raw/encounters/encounters.csv
gs://<bucket-synthea>/raw/conditions/conditions.csv
```

Si tu veux maîtriser la taille du jeu et pouvoir le régénérer, tu peux aussi **générer les données toi-même** avec Synthea. Le projet permet de définir une population et une graine aléatoire, et propose l’export CSV, qui doit être activé dans la configuration si nécessaire. ([github.com](https://github.com/synthetichealth/synthea?utm_source=openai))

> Les données Synthea sont synthétiques : ce ne sont pas des dossiers de vrais patients. Cela en fait un choix adapté à un projet de démonstration, mais elles ne remplacent pas des données cliniques réelles pour tirer des conclusions médicales. ([github.com](https://github.com/synthetichealth/synthea?utm_source=openai))

## 🧰 Rôle des composants

**Kestra :** orchestre les tâches, leurs dépendances, les reprises et le suivi des exécutions. Il lance les jobs Dataproc, les chargements BigQuery et, si tu automatises cette partie, les commandes dbt.

**Docker Compose :** lance Kestra localement pour le développement. Spark n’est pas hébergé dans les conteneurs Docker : les jobs PySpark s’exécutent sur Dataproc.

**Dataproc Serverless + PySpark :** nettoie et prépare les CSV Synthea, puis écrit les résultats en Parquet dans GCS.

**Google Cloud Storage :** stocke les fichiers bruts et les résultats préparés.

**BigQuery :** accueille les tables de staging chargées depuis GCS, puis les modèles analytiques créés par dbt.

**dbt Fusion :** s’exécute **en local**, depuis ton environnement de développement. Il se connecte à BigQuery pour y exécuter les modèles SQL et les tests.

> **À retenir :** dbt Fusion ne s’exécutera pas automatiquement sur ton ordinateur simplement parce que Kestra a terminé les jobs Dataproc. Pour cette première version, tu peux exécuter le flow Kestra, puis lancer dbt Fusion localement. L’automatisation complète pourra être ajoutée ensuite en faisant exécuter dbt par Kestra dans un environnement approprié.

## 📁 Organisation du dépôt

```text
01-synthea-kestra-dataproc-dbt/
├── docker-compose.yml
├── kestra/
│   └── flows/
│       └── synthea_pipeline.yml
├── spark/
│   └── jobs/
│       ├── clean_patients.py
│       ├── prepare_encounters.py
│       └── prepare_conditions.py
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_patients.sql
│   │   │   ├── stg_encounters.sql
│   │   │   └── stg_conditions.sql
│   │   └── marts/
│   │       ├── dim_patients.sql
│   │       ├── fact_encounters.sql
│   │       └── condition_indicators.sql
│   ├── tests/
│   └── README.md
├── tests/
├── README.md
└── .env.example
```

Les identifiants et secrets de connexion ne doivent pas être versionnés. Ajoute la configuration locale dbt contenant des secrets au `.gitignore`, et fournis un fichier d’exemple sans identifiants sensibles.

## 🔄 Étapes du pipeline

1. Télécharger le jeu Synthea au format CSV depuis la page officielle, ou générer les données avec le dépôt Synthea.
2. Sélectionner les fichiers `patients.csv`, `encounters.csv` et `conditions.csv` pour la première version.
3. Vérifier les en-têtes, le format et la présence des fichiers.
4. Déposer les CSV dans le dossier `raw/` du bucket GCS.
5. Lancer depuis Kestra les jobs PySpark sur Dataproc Serverless.
6. Nettoyer les données : types, valeurs manquantes, doublons et règles de qualité.
7. Écrire les résultats préparés en Parquet dans `gs://<bucket>/curated/`.
8. Charger les fichiers préparés dans des tables de staging BigQuery.
9. Exécuter localement dbt Fusion pour construire les modèles analytiques dans BigQuery.
10. Lancer les tests dbt et consulter les résultats, les logs et les états d’exécution.

## 📈 Analyses à présenter

- Nombre de patients par tranche d’âge ou région.
- Nombre de rencontres médicales par période.
- Principales conditions médicales par groupe démographique.
- Contrôles de qualité : doublons, champs manquants et volumes traités.
- Tests dbt sur les clés uniques, les valeurs non nulles et les relations entre modèles.

## ✅ Livrables portfolio

- Un dépôt Git contenant les flows Kestra, les scripts PySpark et le projet dbt.
- Un `docker-compose.yml` permettant de lancer Kestra localement.
- Un README contenant les liens vers Synthea, les fichiers sélectionnés, l’architecture et les instructions.
- Un schéma du pipeline et des captures des exécutions Kestra.
- Des résultats de tests dbt et des exemples de requêtes ou un tableau de bord BigQuery.
- Une description claire des choix d’exécution : données Synthea en CSV, PySpark sur Dataproc et dbt Fusion lancé localement.

## 📌 Positionnement du projet

Tu montres que tu sais combiner orchestration, calcul distribué avec PySpark, stockage cloud, entrepôt BigQuery et transformations analytiques avec dbt Fusion.

**Docker sert à lancer Kestra en local.** Dataproc est le service cloud qui exécute Spark. **dbt Fusion reste local** dans cette version, mais transforme les données directement dans BigQuery. Pour démarrer simplement, télécharge les CSV Synthea et utilise `patients.csv`, `encounters.csv` et `conditions.csv`.
