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


# 📋 NOTE 2 — PROJET PRINCIPAL  
## 🏥 Synthea Health Data Pipeline — Bruin, Dataproc Serverless, PySpark et BigQuery

📂 **Repo :** `DEHUB/01-synthea-bruin-gcp-pipeline/`  
🎯 **Quand :** octobre–novembre 2026  
💡 **Objectif :** construire un pipeline de données de santé synthétiques sur GCP, orchestré avec Bruin, transformé avec PySpark sur Dataproc Serverless, puis modélisé dans BigQuery.

### 💡 Pitch recruteur

> « J’ai construit un pipeline de données synthétiques générées par Synthea, orchestré avec Bruin. Les traitements distribués sont écrits en PySpark et exécutés sur Dataproc Serverless. Les données préparées et les modèles analytiques sont stockés dans BigQuery. Le pipeline inclut des dépendances, des contrôles de qualité, une historisation SCD2 et une configuration IAM documentée. Les données utilisées sont exclusivement synthétiques. »

## 🏗️ Architecture

```text
Synthea
  → fichiers CSV ou FHIR synthétiques
        │
        ▼
Cloud Storage — zone raw
        │
        ▼
Bruin — assets, dépendances, qualité et orchestration
        │
        ▼
Dataproc Serverless — exécution des traitements PySpark
        │
        ├──→ Cloud Storage — fichiers préparés, par exemple en Parquet
        │
        ▼
BigQuery — staging, core et marts
        │
        ▼
Requêtes BigQuery ou Looker Studio
        ⚠️ Mention visible : « Données synthétiques »
```

- **Bruin** organise le pipeline et peut exécuter les assets SQL BigQuery. Il remplace Dataform dans cette version du projet.
- **Dataproc Serverless** fournit le moteur Spark sans cluster persistant à administrer.
- **PySpark** réalise les traitements distribués : parsing, typage, nettoyage et déduplication.
- **BigQuery** héberge les données préparées et les tables analytiques.

**À décider et tester tôt :** le contrat de sortie de PySpark — par exemple Parquet sur GCS puis chargement ou lecture dans BigQuery. Ne présume pas que Spark écrit automatiquement dans BigQuery : valide le connecteur, le runtime et les permissions du compte de service.

**Dataform est optionnel.** Pour éviter deux outils qui orchestrent ou gèrent les mêmes tables, utilise Bruin seul dans la version principale. Tu peux ajouter Dataform plus tard si tu veux comparer les approches.

## 🔄 Étapes et livrables

| Étape | Outil | Livrable |
|---|---|---|
| 1. Génération | Synthea | Export synthétique reproductible et schémas inventoriés |
| 2. Stockage source | Cloud Storage | Fichiers organisés sous `raw/` |
| 3. Définition du pipeline | Bruin | Assets, dépendances et configuration validés |
| 4. Traitement distribué | Dataproc Serverless + PySpark | Données nettoyées et contrat de sortie documenté |
| 5. Modélisation | Bruin + BigQuery SQL | Tables staging, core et marts |
| 6. Historisation | Bruin + BigQuery SQL, ou PySpark si justifié | SCD2 démontrée et testée |
| 7. Qualité et orchestration | Bruin, Cloud Logging | Contrôles, journaux et exécutions documentés |
| 8. Restitution | BigQuery / Looker Studio | Requêtes ou dashboard indiquant « données synthétiques » |

Bruin documente un asset `dataproc_serverless.pyspark` pour exécuter du PySpark sur Dataproc Serverless, ainsi que des assets SQL BigQuery. La connexion Dataproc et son espace de travail GCS sont à configurer selon la documentation et ton environnement GCP.

## 📁 Structure du repo

```text
01-synthea-bruin-gcp-pipeline/
├── README.md
├── LEARNINGS.md
├── pipeline.yml
├── pyproject.toml
├── synthea/
│   ├── config/
│   └── generate.sh
├── pipelines/
│   └── synthea/
│       ├── pipeline.yml
│       └── assets/
│           ├── spark/
│           │   └── normalize_patients.py
│           ├── bigquery/
│           │   ├── staging/
│           │   │   └── patients.sql
│           │   ├── core/
│           │   │   └── dim_patient.sql
│           │   └── marts/
│           │       └── patient_summary.sql
│           └── quality/
├── tests/
│   └── fixtures/
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    └── cost_and_security.md
```

## 🧩 Exemples d’assets Bruin

Les exemples illustrent la forme générale ; les noms de connexions, projet, région, buckets et tables sont à adapter à GCP.

**Asset PySpark Dataproc Serverless** — les métadonnées Bruin sont intégrées au fichier Python :

```python
""" @bruin
name: spark.normalize_patients
type: dataproc_serverless.pyspark
connection: dataproc_serverless-default
depends:
  - synthea.raw_patients
timeout: 1h
@bruin """

# Traitement PySpark.
# Définir explicitement les sources, le schéma et la destination.
```

**Asset SQL BigQuery** — SQL et métadonnées Bruin dans le même fichier :

```sql
/* @bruin
name: staging.patients
type: bq.sql
depends:
  - spark.normalize_patients
materialization:
  type: table
columns:
  - name: patient_id
    type: string
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
  patient_id,
  birth_date,
  gender
FROM `mon_projet.raw_synthea.patients`
```

La connexion Dataproc, les credentials et l’espace de travail GCS se configurent séparément. **Ne mets pas de clés de service dans Git.** Bruin peut être utilisé localement pour développer ; pour une exécution planifiée, prévois un déploiement dans Bruin Cloud ou un ordonnanceur externe/CI configuré pour lancer le pipeline.

## 🌟 Point fort : SCD Type 2

Colonnes possibles :

- `patient_id`
- attributs historisés, par exemple `address`
- `valid_from`
- `valid_to`
- `is_current`

Règles à tester :

1. Une seule version courante par patient.
2. Si les attributs suivis ne changent pas, ne pas créer de nouvelle version.
3. Si un attribut change, fermer l’ancienne version et insérer la nouvelle.
4. Rendre les exécutions rejouables sans créer de versions en double.
5. Ne traiter une absence comme une suppression que si la source permet réellement de détecter les suppressions.

**Livrable :** une requête montrant deux versions d’un patient, avec leurs dates de validité. La logique peut être en SQL BigQuery orchestré par Bruin ; ne la mets en PySpark que si cela sert l’objectif d’apprentissage.

## 🧪 Contrôles de qualité

- Identifiants requis non nuls et uniques selon les règles du modèle.
- Dates valides et cohérentes.
- Chaque encounter référence un patient existant.
- Coûts positifs ou nuls, si ces champs existent dans l’export.
- Au plus une version SCD2 courante par clé et absence de périodes incohérentes.
- Nombres de lignes, doublons et rejets mesurés à chaque étape.

Inventorie les fichiers produits par ta version de Synthea : tous les exports ne sont pas nécessairement générés par défaut.

## 🗺️ Plan de travail

**Semaine 1 — Données et fondations**  
Configurer Synthea, inventorier les fichiers, créer les ressources GCS et BigQuery. Définir IAM, région et schémas.

**Semaine 2 — Bruin et PySpark**  
Créer le pipeline Bruin, configurer la connexion Dataproc Serverless et tester un petit job PySpark de bout en bout.

**Semaine 3 — BigQuery et SCD2**  
Créer les assets SQL de staging/core/marts, ajouter les contrôles et tester l’historisation ainsi que le rejeu.

**Semaine 4 — Orchestration et présentation**  
Documenter le déploiement planifié, vérifier les journaux, faire un run complet et préparer le README et le pitch.

## ✅ Checklist de fin

- ☐ Données Synthea identifiées comme synthétiques ; configuration et version documentées.
- ☐ Fichiers source déposés dans Cloud Storage et schémas inventoriés.
- ☐ Asset Bruin PySpark exécuté sur Dataproc Serverless.
- ☐ Sortie PySpark vers GCS ou BigQuery définie et testée.
- ☐ Assets SQL BigQuery Bruin avec dépendances explicites.
- ☐ Qualité des données et SCD2 testées, y compris lors d’un rejeu.
- ☐ Journaux et erreurs consultables.
- ☐ IAM au moindre privilège ; secrets exclus du dépôt.
- ☐ Exécution planifiée ou procédure d’exécution documentée.
- ☐ Coûts surveillés et ressources d’essai supprimables.
- ☐ README et dashboard affichent « données synthétiques ».
- ☐ `LEARNINGS.md` contient au moins cinq problèmes rencontrés et solutions.

## 💸 Coûts et sécurité

Dataproc Serverless facture les ressources consommées ; BigQuery, GCS et le réseau peuvent également engendrer des coûts. Le total dépend de la région, du volume, de la durée des jobs, du shuffle et des requêtes : vérifie les tarifs GCP applicables avant les essais.

Pour limiter les coûts : commence avec peu de données, évite les backfills inutiles, contrôle les requêtes BigQuery, supprime les ressources de démonstration inutilisées et configure des alertes de budget.

Utilise uniquement des données synthétiques, sépare les identités de soumission et d’exécution lorsque nécessaire, applique le moindre privilège et ne stocke aucun secret dans Git.

## 📚 Ressources

- Bruin — [Dataproc Serverless](https://getbruin.com/docs/bruin/platforms/dataproc_serverless.html)
- Bruin — [BigQuery](https://getbruin.com/docs/bruin/platforms/bigquery.html)
- Bruin — [définition des assets](https://getbruin.com/docs/bruin/assets/definition-schema.html)
- Google Cloud — [Dataproc Serverless](https://docs.cloud.google.com/managed-spark/docs/serverless-overview)
- Google Cloud — [batch PySpark](https://docs.cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark)
- Google Cloud — [tarifs Dataproc Serverless](https://cloud.google.com/dataproc-serverless/pricing)
- Google Cloud — [tarifs BigQuery](https://cloud.google.com/bigquery/pricing)
- Synthea — [site officiel](https://synthetichealth.github.io/synthea/)

**Règle d’or :** Bruin orchestre et organise les assets ; PySpark traite les données distribuées ; Dataproc fournit le calcul Spark ; BigQuery héberge les tables analytiques.
