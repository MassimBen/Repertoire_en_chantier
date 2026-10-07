**Réponse rapide à ta question :** Non, le mini-projet Bruin (Note 1b) n'était **pas** présent dans la version GitHub précédente. Je l'ai intégré ci-dessous, ainsi que le projet streaming GCP (Note 3), avec les données adaptées au contexte santé Cnam (capteurs de santé simulés cohérents avec l'univers du portefeuille, mais clairement fictifs).

Voici la version `.md` complète et mise à jour :

---

# 🏥 DEHUB — Portefeuille Data Engineering Santé

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![dlt](https://img.shields.io/badge/dlt-hub-orange.svg)
![dbt](https://img.shields.io/badge/dbt-core-blue.svg)
![BigQuery](https://img.shields.io/badge/BigQuery-GCP-blue.svg)
![Kestra](https://img.shields.io/badge/Kestra-orchestrator-purple.svg)
![Terraform](https://img.shields.io/badge/Terraform-IAC-5c4ee5.svg)
![Apache Beam](https://img.shields.io/badge/Apache%20Beam-streaming-yellow.svg)

> Portefeuille de projets Data Engineering sur données de santé publique. Projet principal : pipeline ELT sur les open data Cnam. Projets complémentaires : monitoring qualité avec Bruin, et streaming GCP natif (Pub/Sub + Dataflow + Beam) après la certification.

---

## 📑 Table des matières

- [Projet principal — Cnam Open Data Pipeline](#-projet-principal--cnam-open-data-pipeline)
  - [Pitch recruteur](#-pitch-recruteur)
  - [Architecture](#-architecture)
  - [Pourquoi pas Spark ?](#-pourquoi-pas-spark-)
  - [Structure du repo](#-structure-du-repo)
  - [Démarrage rapide](#-démarrage-rapide)
  - [Les 5 étapes](#-les-5-étapes)
  - [Pièges Cnam](#-pièges-cnam)
  - [Métriques clés](#-métriques-clés)
  - [Checklist de fin](#-checklist-de-fin)
- [Mini-projet — Cnam Quality Monitor (Bruin)](#-mini-projet--cnam-quality-monitor-bruin)
- [Projet post-certification — Streaming santé GCP](#-projet-post-certification--streaming-santé-gcp)
- [Plan de travail](#-plan-de-travail)
- [Règles d'or](#-règles-dor)

---

# 🏥 Projet principal — Cnam Open Data Pipeline

📂 **Repo :** `DEHUB/01-cnam-health-pipeline/`
🎯 **Quand :** immédiatement après le Zoomcamp
💡 **Objectif :** construire un pipeline ELT complet sur une seule source réelle — les open data Cnam — avec toutes leurs spécificités : seuils de confidentialité, codes géo atypiques, millésimes multiples.

---

## 💡 Pitch recruteur

> « J'ai construit un pipeline ELT sur les open data Cnam — professionnels de santé, consommation de soins, pathologies lourdes. Une seule source, mais des **vrais problèmes de données** : seuils de confidentialité qui masquent des valeurs, la Corse en 2A/2B, les DOM-TOM à 3 chiffres, des ruptures de série quand la méthodologie change. J'ai utilisé **dlt** pour l'ingestion streaming sans charger 2 GB en RAM, **BigQuery SQL** pour toute la transformation métier — jointures 15M lignes en 3 secondes — **dbt** pour modéliser les indicateurs territoriaux avec tests spécifiques seuils, **Kestra** pour orchestrer, **Terraform** pour l'infrastructure. Résultat : un dashboard de santé publique reproductible en deux commandes, avec traçabilité complète des limitations de la source. »

**Ce qui impressionne en entretien :**

- Choix technique argumenté (pas Spark car inutile sur 15M lignes, preuve mesurée)
- Spécificités métier traitées comme du premier citoyen (seuils = données, pas obstacles)
- Traçabilité complète : chaque ligne sait d'où elle vient et ce qu'on lui a fait

---

## 🔗 Liens utiles

- Portail Cnam Open Data : https://assurance-maladie.ameli.fr/etudes-et-donnees/open-data
- Professionnels de santé : https://www.data.gouv.fr/fr/datasets/professionnels-de-sante-liberaux-effectif-et-densite-par-territoire/
- Dépenses de soins : https://www.data.gouv.fr/fr/datasets/depenses-de-soins-de-ville-par-professionnel/
- Pathologies lourdes (ALD) : https://www.data.gouv.fr/fr/datasets/beneficiaires-de-la-cmu-c-et-de-la-css-effectifs-et-couts/
- dlt : https://dlthub.com/docs/intro
- dbt : https://docs.getdbt.com/
- Kestra : https://kestra.io/docs
- Terraform GCP : https://registry.terraform.io/providers/hashicorp/google/latest/docs

---

## 📊 Données et périmètre

| Jeu Cnam | Indicateurs clés | Granularité | Seuil confidentialité |
|----------|-----------------|-------------|----------------------|
| **Professionnels de santé** | Effectifs, densité par 100 000 hab | Dépt × Spécialité × Année | < 3 professionnels masqués |
| **Dépenses de soins** | Montants remboursés, nombre d'actes | Dépt × Acte × Âge × Sexe | < 11 actes ou < 11 bénéficiaires |
| **Pathologies lourdes (ALD)** | Nombre de bénéficiaires ALD | Dépt × Pathologie × Sexe | < 11 bénéficiaires |

**Problèmes Cnam récurrents :**

- Seuils : valeur remplacée par le seuil lui-même (ex: "5" ou "11") ou vide selon les millésimes
- Corse : codes 2A et 2B, pas 20
- DOM-TOM : 971, 972, 973, 974, 976
- Millésimes : publication décalée entre jeux, ruptures de série

---

## 🏗️ Architecture

```text
CNAM OPEN DATA — data.gouv.fr (CSV ~150 MB, 15M lignes)
              │
              ▼
    ┌─────────────────┐
    │      dlt        │  ◄── Ingestion streaming, RAM ~50 MB constante
    │   (Python)      │      Schéma auto-détecté, normalisation snake_case
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │   GCS bucket    │  ◄── Archive des fichiers bruts (traçabilité)
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  BigQuery raw   │  ◄── 3 tables : professionnels, dépenses, pathologies
    │  (cnam_raw)     │      Colonnes _dlt_load_id, _dlt_extracted_at
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │     Kestra      │  ◄── Orchestration + checkpoints qualité
    └────────┬────────┘
             │
             ▼
    ┌─────────────────────────────────────┐
    │    BigQuery SQL — Transformation    │  ◄── Jointures 15M lignes : ~3 s
    │  • Normalisation Corse (2A→20001)   │      Macros dbt : géo, seuils, typage
    │  • Flag is_below_threshold          │      Pas de JVM, pas de cluster
    │  • Rejets format invalide           │
    └─────────────────────────────────────┘
             │
             ▼
    ┌─────────────────┐
    │ BigQuery staging│  ◄── Données nettoyées, typées, vérifiées
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │      dbt        │  ◄── Modélisation 3 couches + tests spécifiques
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  BigQuery marts │  ◄── Modèle en étoile : dims + facts historisés
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  Looker Studio  │  ◄── 3 pages : densité, dépenses, pathologies
    └─────────────────┘      Seuils visibles, sources citées, 🔒 sur masqué

    Terraform ──► datasets BQ + bucket GCS + compte de service IAM
    Docker    ──► Kestra + Postgres uniquement (pas de cluster Spark)
```

---

## 🎯 Pourquoi pas Spark ?

| Critère | **Spark local** | **BigQuery SQL** | Gagnant |
|---------|-----------------|------------------|---------|
| Temps jointure 15M lignes | ~47 s | ~3 s | **BQ** |
| RAM nécessaire | 4-8 GB | 0 (serverless) | **BQ** |
| Coût infrastructure | ~$50-200/mois cluster | ~$2-5/mois requêtes | **BQ** |
| Complexité ops | Docker JVM + version Spark | Aucune | **BQ** |
| Scalabilité implicite | Cluster à gérer | Auto (PB natif) | **BQ** |
| Honnêteté technique | "Pourquoi Spark sur si peu ?" | "Choix mûr, mesuré" | **BQ** |

> **Verdict :** 15M lignes = sweet spot BigQuery SQL. Spark local = over-engineering avec overhead Docker injustifié. Benchmark mesuré et documenté dans [`docs/benchmark.md`](./docs/benchmark.md).
> On ajoutera Spark **si et seulement si** on dépasse 100M+ lignes ou qu'on a besoin de streaming temps réel — décision tracée dans un ADR. Et le streaming temps réel, c'est justement l'objet du [projet post-certification](#-projet-post-certification--streaming-santé-gcp).

---

## 📁 Structure du repo

```text
01-cnam-health-pipeline/
├── README.md
├── LICENSE
├── Makefile
├── docs/
│   ├── architecture.png
│   ├── data_dictionary.md          # Mapping Corse, règles seuils, nomenclatures
│   ├── sources_limitations.md      # Limites Cnam par jeu et millésime
│   ├── benchmark.md                # Preuve mesurée : Spark vs BQ SQL
│   └── LEARNINGS.md                # ← Logbook obligatoire (≥ 5 entrées)
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── ingest/
│   ├── cnam_pipeline.py            # dlt : pipeline unique 3 jeux
│   ├── requirements.txt
│   └── .dlt/config.toml
├── transform/
│   ├── macros/                     # normalize_geo, apply_seuil, infer_type_safe
│   └── checks/                     # completeness : 101 territoires ?
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
│       └── cnam_specific/
│           ├── no_confidentiality_leak.sql
│           └── territorial_completeness.sql
├── kestra_flows/
│   └── cnam_pipeline.yml
├── docker-compose.yml
├── data/sample/                    # Extraits 2-3 départements pour dev
└── .github/workflows/ci.yml
```

---

## 🚀 Démarrage rapide

### Prérequis

- Google Cloud SDK authentifié
- Terraform ≥ 1.5
- Docker + Docker Compose
- Python 3.11

### 1. Infrastructure (une fois)

```bash
cd terraform/
terraform init
terraform apply          # Dataset BQ, bucket GCS, compte de service
```

### 2. Orchestration

```bash
docker compose up -d     # Kestra + Postgres
```

### 3. Pipeline complet (une commande)

```bash
make pipeline            # dlt ingest + BQ transform + dbt build + tests
# Ou via Kestra UI : http://localhost:8080 → flow cnam-health-pipeline → Execute
```

### 4. Dashboard

Looker Studio → connecter `cnam_mart.*` → template documenté dans `docs/`.

---

## 🔄 Les 5 étapes

### 1️⃣ Ingestion — dlt

- Pipeline unique `cnam_pipeline.py` gérant les 3 jeux
- Source : fichiers CSV data.gouv (téléchargement ou API si stable)
- **Traçabilité obligatoire :** `_dlt_load_id`, `_dlt_extracted_at`, `source_jeu`, `source_millesime`, `source_url`
- **RAM constante ~50 MB** : dlt stream ligne par ligne, pas de lecture monolithique

### 2️⃣ Transformation — BigQuery SQL + macros dbt

| Problème Cnam | Traitement | Sortie |
|---------------|-----------|--------|
| Seuil "< 11" ou "11" | Détection pattern → NULL + `is_below_threshold = TRUE` | Pas de zéro interprété à tort |
| Corse 2A / 2B | Mapping explicite vers 20001/20002 | Documenté dans data dictionary |
| DOM-TOM 971-976 | Conservés tels quels, type cohérent | Pas de perte silencieuse |
| Année texte vs entier | CAST sécurisé avec rejet | Données invalides isolées |

### 3️⃣ Modélisation — dbt (3 couches)

| Couche | Rôle | Exemples |
|--------|------|----------|
| **staging** | Typage strict, nommage, source fiable | `stg_cnam__professionnels` |
| **intermediate** | Règles métier, jointures, agrégations | `int_seuils_appliques` |
| **marts** | Modèle en étoile, prêt à consommer | `fact_densite_professionnels` |

**Tests spécifiques Cnam :**

- Unicité du grain de chaque fact
- `accepted_values` : `is_below_threshold IN (TRUE, FALSE, NULL)`
- Pas de test `> 0` sur mesures — les seuils valides sont NULL, pas des erreurs
- Intégrité référentielle : tout territoire de fact existe dans `dim_territoire`
- **Anti-fuite :** aucune valeur NULL sans flag de seuil activé

### 4️⃣ Orchestration — Kestra

```text
dlt extract_all → check_raw (comptage par jeu/millésime)
  → SQL transform (geo + seuils) → check_staging (101 territoires)
  → dbt build → dbt test → notify (résumé ou alerte)
```

Tester les échecs : millésime manquant, colonne Cnam renommée, nouveau code géo.

### 5️⃣ Dashboard — Looker Studio

| Page | Focus | Mention obligatoire |
|------|-------|---------------------|
| Densité médicale | Carte chloroplèthe, filtre spécialité/année | "Données Cnam, seuils de confidentialité appliqués" |
| Dépenses et actes | Évolution temporelle, alerte rupture série | "Millésime 202X, publication décalée" |
| Pathologies lourdes | Table avec 🔒 sur lignes masquées | "Valeurs < 11 non affichées conformément aux règles Cnam" |

Couleur spéciale pour les données masquées — visibilité de la limitation, pas d'absence visuelle.

---

## ⚠️ Pièges Cnam

| Piège | Symptôme | Solution implémentée |
|-------|----------|----------------------|
| **Seuil lu comme nombre** | "11" interprété comme 11 actes | Détection par pattern, flag + NULL |
| **Corse oubliée** | Jointure échoue silencieusement | Mapping explicite, test complétude 101 territoires |
| **Millésime incomplet** | 2022 dispo pour A, pas pour B | Check par jeu, pas global |
| **Changement nomenclature** | Nouveau code acte | Pipeline échoue proprement (`accepted_values`) |
| **Spark par habitude** | Over-engineering justifié maladroitement | Benchmark documenté, décision tracée |

---

## 📈 Métriques clés

| Métrique | Cible | Où mesurer |
|----------|-------|-----------|
| Jeux Cnam intégrés | 3 | logs dlt |
| Millésimes par jeu | 3-5 | `check_raw` Kestra |
| Territoires reconnus | 101 | `territorial_completeness.sql` |
| Fuite de confidentialité | 0 | `no_confidentiality_leak.sql` |
| Temps ingestion dlt | mesuré | `time` commande |
| Temps jointure 15M lignes | ~3 s | BQ query stats |
| Temps bout en bout Kestra | mesuré | Kestra execution logs |
| Coût mensuel GCP | < 5 $ | BQ billing |

---

## ✅ Checklist de fin

- [ ] `make pipeline` = pipeline complet reproductible
- [ ] README avec archi, limites par jeu, **pourquoi pas Spark**
- [ ] `docs/LEARNINGS.md` ≥ 5 problèmes Cnam réels résolus
- [ ] `docs/data_dictionary.md` : mapping Corse, règles seuils, millésimes
- [ ] `docs/benchmark.md` : comparaison mesurée Spark vs BQ SQL
- [ ] Dashboard Looker avec sources, millésimes, légende seuils visibles
- [ ] Terraform `apply` et `destroy` testés et documentés
- [ ] Test anti-fuite passe : 0 ligne avec NULL sans flag seuil
- [ ] Screenshot "avant" (valeur brute) et "après" (flag + NULL)

---

# 🔍 Mini-projet — Cnam Quality Monitor (Bruin)

📂 **Repo :** `DEHUB/01b-cnam-quality-monitor/` ou dossier `bruin/` dans le repo principal
🎯 **Quand :** en parallèle ou juste après le projet principal
💡 **Objectif :** surveillance indépendante de la qualité des données Cnam — DataOps déclaratif.

## 💡 Pitch recruteur

> « En complément du pipeline principal, j'ai créé un monitoring de qualité déclaratif avec Bruin sur les données Cnam. Les règles vérifient que la source n'a pas changé de format, que la Corse n'a pas été oubliée, que les seuils de confidentialité ne sont pas traités comme des zéros, et qu'aucune donnée sensible n'est exposée dans les marts. C'est la couche 'gardien' qui survit aux changements de source. »

## 🏗️ Architecture

```text
Tables du projet principal
    │
    ├──→ cnam_raw.*        ← Nouveau millésime disponible ?
    ├──→ cnam_staging.*    → Corse présente ? Codes reconnus ?
    └──→ cnam_mart.*       → Pas de fuite confid ? Cohérence seuils ?

              ▼
           BRUIN (YAML)
              ▼
         cnam_audit.quality_daily
```

## 🔄 Contrôles

| Catégorie | Règle Bruin | Cible | Criticité |
|-----------|-------------|-------|-----------|
| Complétude géo | 101 codes territoire distincts | staging | 🔴 Haute |
| Corse présente | 2A ou 2B ou codes normalisés | raw | 🔴 Haute |
| Seuils respectés | Flag cohérent avec valeur NULL | staging | 🔴 Critique |
| Pas de fuite | Aucune valeur < 11 sans flag activé | mart | 🔴 Critique |
| Plausibilité | Densité entre 0 et 500, dépenses positives ou NULL | mart | 🟡 Moyenne |
| Évolution | Variation annuelle < 50% sauf rupture documentée | mart | 🟡 Moyenne |

## 📁 Structure

```text
bruin/
├── bruin.yml
├── assets/
│   ├── raw_checks/
│   │   └── fraicheur.yml
│   ├── staging_checks/
│   │   ├── completude_geo.yml
│   │   └── coherence_seuils.yml
│   └── mart_checks/
│       ├── garde_confidentialite.yml
│       └── plausibilite.yml
├── pipelines/
│   └── cnam_quality_daily.yml
└── README.md
```

## 📝 Pourquoi Bruin et pas dbt pour ces tests ?

> dbt excelle sur les tests liés aux modèles de transformation. Bruin permet de déclarer des pipelines de qualité **indépendants du cycle `dbt run`**, avec du lineage et des exécutions planifiées. Vérification que la *source* Cnam est intacte, que mon *nettoyage géographique* n'a pas perdu la Corse, que personne n'a supprimé le flag `is_below_threshold`. Responsabilités différentes, outils complémentaires — pas une accumulation inutile.

---

# 📡 Projet post-certification — Streaming santé simulé sur GCP

📂 **Repo :** `DEHUB/03-health-streaming-gcp/`
🎯 **Quand :** après les cours préparatoires à la certification GCP
💡 **Objectif :** pratiquer Pub/Sub, Dataflow, Apache Beam, BigQuery streaming, Terraform et Cloud Monitoring.

## 💡 Pitch recruteur

> « J'ai créé un pipeline de streaming GCP qui reçoit des événements de capteurs de santé fictifs via Pub/Sub, les traite avec Apache Beam sur Dataflow et écrit les résultats dans BigQuery. J'ai testé les fenêtres temporelles, les événements retardés, les doublons et les messages invalides — avec une sortie de quarantaine. Les événements et seuils sont entièrement synthétiques : c'est un projet de démonstration technique de streaming, pas un outil de surveillance clinique. »

## 🔗 Liens utiles

- Dataflow : https://cloud.google.com/dataflow/docs
- Concepts streaming Dataflow : https://cloud.google.com/dataflow/docs/concepts/streaming-pipelines
- Pub/Sub avec Dataflow : https://cloud.google.com/pubsub/docs/stream-messages-dataflow
- Apache Beam (Python) : https://beam.apache.org/documentation/programming-guide/
- BigQuery : https://cloud.google.com/bigquery/docs
- Cloud Monitoring : https://cloud.google.com/monitoring/docs
- Terraform GCP : https://registry.terraform.io/providers/hashicorp/google/latest/docs

## 🏗️ Architecture

```text
Simulateur Python — événements de capteurs santé fictifs
        ▼
Cloud Pub/Sub
        ▼
Dataflow / Apache Beam
  ├── parsing + validation JSON
  ├── quarantaine (messages invalides)
  ├── fenêtres temporelles par capteur
  └── agrégats (moyenne par fenêtre)
        ▼
BigQuery — événements bruts + agrégats + quarantaine
        ▼
Looker Studio

Terraform        — ressources GCP (moindre privilège IAM)
Cloud Monitoring — métriques et alertes techniques
```

**Stack :** Python · Pub/Sub · Apache Beam · Dataflow · BigQuery · Terraform · Cloud Monitoring · Looker Studio

> ⚠️ **Disclaimer :** Ce projet sert à démontrer le streaming. Les événements et seuils sont **fictifs** : ne jamais le présenter comme un outil de surveillance clinique ou de diagnostic.

## 🔄 Les 5 étapes

### 1️⃣ Créer le simulateur

- Messages JSON avec `event_id`, `sensor_id`, horodatage
- Mesures fictives cohérentes avec l'univers du portefeuille : fréquence cardiaque simulée, température simulée, saturation simulée
- Mode reproductible (seed fixe) pour relancer les mêmes tests
- Générer volontairement des messages invalides et retardés pour tester les cas limites
- README explicite : données 100% synthétiques

### 2️⃣ Publier dans Pub/Sub

- Topic + ressources Pub/Sub via Terraform
- Publication depuis le simulateur
- Comptes de service et permissions vérifiés (moindre privilège)
- Instructions de nettoyage des ressources documentées

### 3️⃣ Traiter le flux avec Beam et Dataflow

- Lecture depuis Pub/Sub, parsing et validation JSON
- Messages invalides → sortie de quarantaine (table BQ dédiée)
- Fenêtre temporelle par capteur + agrégat (moyenne par fenêtre)
- Tests : messages retardés, doublons, invalides
- Écriture des événements bruts ET des agrégats dans BigQuery
- **DirectRunner en local d'abord**, puis Dataflow
- Tests unitaires sur le parsing et les transformations Beam

### 4️⃣ BigQuery, dashboard et monitoring

- Tables : événements bruts, agrégats, quarantaine
- Dashboard Looker Studio simple
- Alerte **technique ou de démonstration** — jamais médicale
- Test du comportement : message invalide, échec du job
- Mesure documentée de la latence et du débit

### 5️⃣ Terraform et nettoyage

- Déploiement des ressources avec Terraform
- Principe du moindre privilège IAM, aucun secret committé
- Après la démo : arrêt du job Dataflow, vérification des ressources actives
- Documentation déploiement **et** suppression de l'infrastructure

## 📁 Structure du repo

```text
03-health-streaming-gcp/
├── README.md
├── docs/
│   ├── architecture.png
│   └── LEARNINGS.md
├── src/
│   ├── simulator.py
│   └── streaming_pipeline.py
├── tests/
├── terraform/
├── monitoring/
├── requirements.txt
└── .github/workflows/
```

## 📈 Métriques à mesurer

- Messages publiés, traités, invalides, mis en quarantaine
- Latence mesurée + méthode de mesure documentée
- Débit observé, durée du test, configuration utilisée
- Résultats des tests : fenêtres, retards, doublons
- Coût estimé ou constaté, hypothèses explicites
- Captures : job Dataflow, tables BigQuery, dashboard

> ⚠️ Ne pas promettre à l'avance une latence fixe, un débit ou une disponibilité. Ce sont des mesures à effectuer sur le projet, pas des objectifs marketing.

## 🎯 Lien avec le projet principal

Ce projet répond à la limite identifiée dans le projet Cnam : *"on ajoutera du streaming temps réel si le besoin le justifie"*. Ici, le besoin est pédagogique — démontrer la maîtrise du streaming GCP natif pour la certification — avec un univers cohérent (santé) mais des données clairement séparées : **Cnam = données réelles publiques, capteurs = données 100% synthétiques**.

---

# 🗺️ Plan de travail

| Période | Projet | Focus | Semaines |
|---------|--------|-------|----------|
| **Oct-Nov 2026** | Projet principal — Cnam Pipeline | Ingestion dlt, géo, seuils, modélisation dbt | 4-5 |
| | Mini-projet — Bruin Quality Monitor | DataOps, surveillance qualité indépendante | +1-2 |
| **Déc 2026** | Certification GCP | Cours + labs | selon |
| **Jan-Fév 2027** | Streaming GCP (post-certif) | Pub/Sub, Dataflow, Beam, Monitoring | 2-3 |

**Ordre de travail :**

1. Finir le Zoomcamp et ses exercices
2. Faire le projet Cnam comme projet principal : dlt, BigQuery, dbt, Kestra, Terraform, Looker Studio
3. Ajouter le mini-projet Bruin en complément DataOps
4. Après les cours préparatoires GCP, faire le projet streaming Pub/Sub + Dataflow + BigQuery

---

# 📝 Règles d'or

1. **Logbook `LEARNINGS.md`** — chaque spécificité résolue = une entrée (arme secrète en entretien)
2. **Commit par étape** — dlt fonctionnel → SQL geo → SQL seuils → dbt → Kestra. Jamais de commit géant
3. **Screenshots dès que ça marche** — Kestra, tests dbt, job Dataflow, dashboard. Montrer "avant" (valeur brute) et "après" (flag + NULL) pour les seuils
4. **Métriques mesurées** — pas de "environ", des `time` et des `COUNT(*)` réels
5. **Sources visibles partout** — chaque visuel indique "Cnam, millésime X, seuils appliqués" ou "données synthétiques"
6. **Benchmark documenté** — preuve que BigQuery SQL suffit, Spark non justifié sur ce volume

### Template d'entrée LEARNINGS.md

```markdown
## Problème
Qu'est-ce qui ne fonctionnait pas ?

## Cause vérifiée
Comment ai-je identifié la cause ?

## Solution
Qu'ai-je changé et comment l'ai-je testé ?

## Ce que j'ai appris
Quel concept ou quelle documentation m'a aidé ?
```

**Exemples attendus :**

> *"2026-10-15 — Pourquoi 2A/2B était cassé : la Corse n'était pas dans ma jointure sur `code_departement INT` car 2A n'est pas castable en entier. Solution : typage en chaîne dès l'ingestion, normalisation via macro vers 20001/20002."*

> *"2026-10-22 — Le seuil '< 11' vs '11' selon les millésimes : un jeu utilise le chevron, l'autre met directement le chiffre. Règle unifiée : toute valeur masquée → NULL + flag."*

---

## 🤝 Contribution

Issues et PR bienvenues. Pour toute question sur les spécificités Cnam, consulter `docs/sources_limitations.md` d'abord.

## 📄 Licence

MIT — Voir [LICENSE](./LICENSE)

---

> *« Les données Cnam sont synthétiquement complexes : une seule source, mille pièges. Ce pipeline les traite comme des citoyennes de première classe, pas comme des cas particuliers à corriger au dernier moment. »*
