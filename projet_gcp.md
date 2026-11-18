# 📋 PROJET 1 — PIPELINE BATCH SYNTHEA

## 🏥 Synthea Batch Pipeline — Kestra, Dataproc, PySpark, GCS, BigQuery et dbt Fusion

| | |
|:---|:---|
| **📂 Repo** | `DEHUB/01-synthea-gcp-batch-pipeline/` |
| **☁️ Projet GCP** | `note` — ton projet Google Cloud principal |
| **🎯 Objectif** | Construire un pipeline batch reproductible à partir de fichiers CSV de santé synthétiques Synthea |
| **🧰 Technologies** | Kestra, Dataproc Serverless, PySpark, Google Cloud Storage, BigQuery et dbt Fusion exécuté localement |

---

## 💡 Pitch recruteur

> « J’ai construit un pipeline batch de bout en bout à partir des données synthétiques Synthea. Kestra orchestre les étapes, Dataproc Serverless exécute les traitements PySpark, et Google Cloud Storage conserve les fichiers bruts et préparés. Les résultats sont chargés dans BigQuery, où dbt Fusion, exécuté localement, construit les modèles analytiques et lance les tests de qualité. »

---

## 🧭 Architecture déployée sur GCP (`note`)

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                         KESTRA — orchestration                          │
│       Validation des fichiers, exécution des jobs, suivi et reprises    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                 GOOGLE CLOUD STORAGE — zone raw                         │
│           patients.csv │ encounters.csv │ conditions.csv               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              DATAPROC SERVERLESS — traitements PySpark                  │
│       Nettoyage, typage, contrôles qualité et préparation des données   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                GOOGLE CLOUD STORAGE — zone curated                      │
│                  Fichiers préparés au format Parquet                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  BIGQUERY — tables de staging                           │
│          patients │ encounters │ conditions                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│             dbt Fusion — installé et exécuté localement                  │
│       Modèles staging et marts, tests de qualité, documentation          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  BIGQUERY — tables analytiques                          │
│     dim_patients │ fact_encounters │ condition_indicators                │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                Requêtes analytiques ou tableau de bord                  │
└─────────────────────────────────────────────────────────────────────────┘
```

**Kestra peut soumettre les traitements PySpark à Dataproc Serverless** et suivre leur exécution. Dataproc exécute les jobs à la demande, sans cluster Spark permanent à gérer. ([kestra.io](https://kestra.io/plugins/plugin-gcp/google-cloud-dataproc-batches/io.kestra.plugin.gcp.dataproc.batches.pysparksubmit?utm_source=openai))

> **Important :** dans la première version, Kestra orchestre le pipeline cloud, mais dbt Fusion reste exécuté séparément depuis ton ordinateur. Il ne sera pas lancé automatiquement par Kestra à moins d’ajouter plus tard un environnement d’exécution configuré pour dbt Fusion.

---

## 🔗 Sources de données Synthea

Synthea est un générateur open source de dossiers de santé synthétiques. Il fournit des données d’exemple au format CSV ainsi que d’autres formats. La page de téléchargement propose notamment des échantillons CSV ; tu peux aussi générer tes propres données avec le logiciel. ([synthetichealth.github.io](https://synthetichealth.github.io/synthea/?utm_source=openai))

| Ressource | Lien | Utilisation |
|---|---|---|
| **Téléchargements Synthea** | [synthetichealth.github.io/downloads](https://synthetichealth.github.io/downloads.html) | Télécharger un jeu d’exemple CSV |
| **Page officielle Synthea** | [synthetichealth.github.io/synthea](https://synthetichealth.github.io/synthea/) | Présentation et accès aux données |
| **Dépôt GitHub** | [github.com/synthetichealth/synthea](https://github.com/synthetichealth/synthea) | Générer des données et consulter les instructions |

### Fichiers recommandés pour commencer

| Fichier | Contenu | Utilisation prévue |
|---|---|---|
| `patients.csv` | Informations synthétiques sur les patients, démographie, naissance et décès éventuel | Dimension patients, tranches d’âge et analyses démographiques |
| `encounters.csv` | Rencontres médicales, dates, types et patients associés | Nombre de rencontres par période ou type |
| `conditions.csv` | Conditions ou problèmes de santé associés aux patients | Indicateurs de conditions par groupe démographique |

Fichiers à ajouter éventuellement dans une version ultérieure :

| Fichier | Utilisation possible |
|---|---|
| `observations.csv` | Mesures et résultats observés |
| `medications.csv` | Médicaments associés aux patients ou aux rencontres |
| `procedures.csv` | Procédures réalisées |
| `organizations.csv` | Organisations ou établissements |
| `providers.csv` | Professionnels de santé |

Les fichiers et leurs colonnes peuvent varier selon le jeu utilisé ou les options de génération. **Inspecte les en-têtes CSV avant de définir les schémas PySpark et les tables BigQuery.**

> Les données Synthea sont synthétiques : elles ne correspondent pas à de vrais patients. Elles conviennent à une démonstration technique, mais ne permettent pas de tirer des conclusions médicales sur une population réelle.

---

## 📊 Périmètre des données

| Domaine | Indicateurs envisagés | Grain à vérifier | Particularités |
|---|---|---|---|
| **Patients** | Nombre de patients, tranches d’âge, répartition par région si disponible | Patient | Vérifier les colonnes, les valeurs manquantes et les éventuelles dates de décès |
| **Rencontres** | Nombre de rencontres par période ou type | Rencontre | Vérifier l’identifiant, les dates et les types disponibles |
| **Conditions** | Conditions les plus fréquentes par groupe démographique | Condition par patient ou rencontre | Vérifier les clés et les champs réellement présents |

Ne présume pas que chaque fichier contient toutes les colonnes attendues. Les relations entre les tables doivent être construites uniquement lorsque les identifiants nécessaires sont présents et cohérents.

---

## 🧰 Rôle des composants

| Composant | Rôle dans le projet |
|---|---|
| **Kestra** | Orchestre les étapes, vérifie les entrées, lance les traitements Dataproc et suit les exécutions. |
| **Docker Compose** | Lance Kestra localement pour le développement. |
| **Dataproc Serverless** | Exécute les jobs PySpark à la demande. |
| **PySpark** | Nettoie, type et prépare les fichiers Synthea. |
| **Google Cloud Storage** | Stocke les fichiers bruts et les fichiers préparés en Parquet. |
| **BigQuery** | Stocke les tables de staging et les modèles analytiques. |
| **dbt Fusion** | S’exécute localement, se connecte à BigQuery et crée les modèles analytiques et les tests. |

**Note sur Docker :** Docker Compose sert à lancer Kestra localement. Les jobs Spark de cette architecture s’exécutent sur Dataproc Serverless, et non dans un cluster Spark Dockerisé.

---

## 🔄 Étapes du pipeline

### 1. Récupération des données

Télécharger un petit jeu d’exemple CSV Synthea ou générer ses propres données. Pour commencer, utiliser `patients.csv`, `encounters.csv` et `conditions.csv`.

### 2. Dépôt dans GCS

Conserver les fichiers originaux dans une zone brute, sans les modifier.

```text
gs://<bucket-synthea>/raw/patients/patients.csv
gs://<bucket-synthea>/raw/encounters/encounters.csv
gs://<bucket-synthea>/raw/conditions/conditions.csv
```

### 3. Validation des entrées

Avant d’envoyer un job Dataproc, contrôler :

- la présence des fichiers attendus ;
- la lisibilité des CSV ;
- la présence des en-têtes ;
- les colonnes obligatoires pour le traitement ;
- le volume de lignes reçu, si cette information est disponible.

### 4. Traitement PySpark sur Dataproc Serverless

Les jobs PySpark préparent les données :

- conversion explicite des types ;
- traitement documenté des valeurs manquantes ;
- détection ou traitement des doublons ;
- contrôles des clés disponibles ;
- comptage des lignes avant et après traitement.

### 5. Écriture des résultats préparés

Écrire les fichiers préparés au format Parquet dans une zone séparée de GCS :

```text
gs://<bucket-synthea>/curated/patients/
gs://<bucket-synthea>/curated/encounters/
gs://<bucket-synthea>/curated/conditions/
```

### 6. Chargement dans BigQuery

Charger les résultats préparés dans des tables de staging BigQuery, en conservant une séparation claire entre les tables d’entrée et les modèles analytiques.

### 7. Transformation avec dbt Fusion

Depuis le dossier dbt du dépôt, exécuter dbt Fusion localement. dbt se connecte à BigQuery, construit les modèles et lance les tests.

### 8. Vérification et présentation

Consulter les statuts et les logs Kestra, vérifier les tests dbt et interroger les tables finales dans BigQuery. Un tableau de bord peut être ajouté en complément.

---

## 🧱 Modélisation dbt

Organisation possible du projet :

```text
dbt/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── stg_patients.sql
│   │   ├── stg_encounters.sql
│   │   └── stg_conditions.sql
│   └── marts/
│       ├── dim_patients.sql
│       ├── fact_encounters.sql
│       └── condition_indicators.sql
├── tests/
└── README.md
```

### Modèles staging

- Standardiser les noms de colonnes.
- Convertir les types nécessaires.
- Conserver les identifiants utiles aux relations.
- Rendre explicites les valeurs manquantes et les règles appliquées.

### Modèles analytiques

- `dim_patients` : données patient préparées pour l’analyse.
- `fact_encounters` : indicateurs au grain rencontre.
- `condition_indicators` : indicateurs de conditions, selon les dimensions réellement disponibles.

### Tests dbt

Prévoir des tests adaptés aux schémas observés :

- valeurs obligatoires non nulles ;
- identifiants uniques lorsque le grain le permet ;
- intégrité référentielle lorsque les clés sont disponibles ;
- valeurs acceptées lorsque les champs sont catégoriels ;
- cohérence des volumes ou de règles métier simples.

---

## 🧹 Règles de qualité des données

| Règle | Application |
|---|---|
| **Schémas** | Vérifier les noms et types de colonnes à l’arrivée des fichiers. |
| **Valeurs manquantes** | Mesurer les valeurs absentes et documenter les traitements effectués. |
| **Doublons** | Définir une clé de déduplication seulement si elle est justifiée par les données. |
| **Relations** | Vérifier les liens entre patients, rencontres et conditions uniquement si les identifiants existent. |
| **Traçabilité** | Conserver les volumes avant et après préparation et la date du traitement. |
| **Données synthétiques** | Présenter les résultats comme une démonstration technique, jamais comme des statistiques médicales réelles. |

---

## ⚙️ Orchestration et exécution de dbt

Pour la première version, les deux étapes sont documentées et exécutées séparément :

1. Lancer le flow Kestra.
2. Vérifier la réussite des jobs Dataproc et le chargement des tables de staging dans BigQuery.
3. Depuis le dépôt, exécuter dbt Fusion localement.
4. Examiner les tests et les tables analytiques.

Une automatisation ultérieure pourra faire exécuter dbt par Kestra, à condition que l’environnement d’exécution de Kestra dispose de dbt Fusion et de la configuration d’accès à BigQuery.

---

## ☁️ Configuration GCP

### Organisation BigQuery suggérée

| Dataset | Contenu | Usage |
|---|---|---|
| `synthea_staging` | Tables chargées depuis les résultats préparés | Entrée des modèles dbt |
| `synthea_mart` | Dimensions, faits et indicateurs | Analyses et tableau de bord |

Les noms peuvent être adaptés à ton projet. Dans BigQuery, les datasets doivent être créés dans une région compatible avec les ressources et les données utilisées.

### Accès et secrets

Prévoir les permissions nécessaires pour que les identités de service utilisées puissent :

- lire ou écrire les objets GCS concernés ;
- soumettre et suivre les jobs Dataproc ;
- exécuter des jobs BigQuery et écrire les tables requises.

Ne versionne pas de clé JSON, de mot de passe ou de secret. Ajoute les fichiers locaux contenant des identifiants au `.gitignore` et fournis uniquement des fichiers d’exemple sans secret.

---

## 📈 Analyses à présenter

- Nombre de patients par tranche d’âge.
- Nombre de patients par région, si les champs nécessaires sont disponibles.
- Nombre de rencontres médicales par période.
- Principales conditions par groupe démographique.
- Doublons et valeurs manquantes par table.
- Volumes de lignes avant et après le nettoyage.
- Résultats des tests dbt.

---

## 📏 Mesures de suivi du pipeline

| Mesure | Pourquoi la suivre |
|---|---|
| Fichiers reçus et traités | Confirmer que chaque exécution a les entrées attendues |
| Lignes avant et après traitement | Repérer une perte ou une augmentation inattendue de données |
| Lignes rejetées ou invalides | Suivre les problèmes de qualité |
| Durée des jobs | Observer le temps de traitement |
| Résultat des tests dbt | Vérifier la validité des modèles analytiques |
| Coûts des services GCP | Surveiller les ressources créées et utilisées |

---

## 📁 Structure du dépôt

```text
01-synthea-gcp-batch-pipeline/
├── README.md
├── .gitignore
├── .env.example
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
│   │   └── marts/
│   ├── tests/
│   └── README.md
├── tests/
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    └── data_quality.md
```

---

## ✅ Checklist de livraison

- [ ] Jeu CSV Synthea récupéré ou généré.
- [ ] En-têtes et colonnes des trois fichiers de départ inspectés.
- [ ] Fichiers déposés dans GCS, dans une zone brute distincte.
- [ ] Flow Kestra exécuté et logs consultés.
- [ ] Jobs PySpark Dataproc exécutés et résultats écrits en Parquet.
- [ ] Tables de staging créées dans BigQuery.
- [ ] dbt Fusion exécuté localement avec une connexion BigQuery fonctionnelle.
- [ ] Tests dbt exécutés et résultats documentés.
- [ ] Aucun secret ni fichier de données volumineux versionné.
- [ ] README, schéma d’architecture et limites du projet documentés.
- [ ] Coûts et ressources GCP pris en compte.

---

## 📌 Positionnement du projet

Ce projet démontre la maîtrise d’un pipeline batch cloud : orchestration avec Kestra, traitement distribué PySpark sur Dataproc Serverless, stockage de fichiers dans GCS, analyse dans BigQuery et modélisation avec dbt Fusion.

**Frontières des outils :** Kestra orchestre ; Dataproc exécute PySpark ; GCS stocke les fichiers ; BigQuery héberge les tables ; dbt Fusion s’exécute localement et transforme les données dans BigQuery.

---

# 📋 PROJET 2 — PIPELINE STREAMING

## ⚡ Synthetic Health Events Pipeline — Pub/Sub, Dataflow et BigQuery

| | |
|:---|:---|
| **📂 Repo** | `DEHUB/02-synthetic-health-streaming-pipeline/` |
| **☁️ Projet GCP** | `note` — ton projet Google Cloud principal |
| **🎯 Objectif** | Construire un pipeline streaming de démonstration à partir d’événements de santé synthétiques |
| **🧰 Technologies** | Pub/Sub, Dataflow, BigQuery et dbt Fusion exécuté localement en option |

---

## 💡 Pitch recruteur

> « J’ai construit un pipeline streaming de démonstration à partir d’événements entièrement synthétiques. Les événements sont publiés dans Pub/Sub, traités et validés par Dataflow, puis stockés dans BigQuery. J’ai documenté les règles de qualité, le traitement des erreurs, la supervision et les compromis liés à la latence, à la fiabilité et au coût. »

---

## 🧭 Architecture déployée sur GCP (`note`)

```text
┌─────────────────────────────────────────────────────────────────────────┐
│              GÉNÉRATEUR D’ÉVÉNEMENTS SYNTHÉTIQUES                       │
│        Création d’événements fictifs suivant un schéma défini            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         PUB/SUB — ingestion                             │
│              Réception et distribution des événements                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     DATAFLOW — traitement streaming                      │
│       Validation, transformation, suivi des erreurs et des volumes      │
└─────────────────────────────────────────────────────────────────────────┘
                      │                         │
                      ▼                         ▼
┌───────────────────────────────┐  ┌──────────────────────────────────────┐
│ BigQuery — événements valides │  │ Sortie d’erreurs / événements rejetés│
└───────────────────────────────┘  └──────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│          dbt Fusion exécuté localement — optionnel                      │
│              Modèles analytiques et tests BigQuery                      │
└─────────────────────────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                 Requêtes analytiques ou tableau de bord                 │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 Objectif

Construire un pipeline qui reçoit des événements fictifs au fil de leur arrivée, les traite avec Dataflow et les stocke dans BigQuery.

Ce projet complète le pipeline batch :

- le projet batch traite des fichiers historiques ;
- le projet streaming traite des événements qui arrivent continuellement.

Les événements sont créés pour la démonstration. Ils ne doivent contenir aucune donnée réelle de patient.

---

## 📥 Événements à générer

Définir un schéma simple et documenté, avec des événements tels que :

| Type d’événement fictif | Exemple d’usage dans le projet |
|---|---|
| Rencontre créée | Suivre le nombre de rencontres reçues au fil du temps |
| Observation ajoutée | Démontrer le traitement d’un événement de mesure |
| Statut modifié | Montrer la gestion de mises à jour |
| Événement invalide | Tester les validations et la gestion des erreurs |

Pour chaque événement, documenter les champs obligatoires, les types attendus et les règles de validation.

---

## 🔄 Étapes du pipeline

### 1. Définition du schéma

Définir la structure des événements et les règles de validation avant de construire le pipeline.

### 2. Génération des événements

Créer un générateur qui produit des données fictives et permet de contrôler le rythme d’émission et les événements invalides.

### 3. Ingestion avec Pub/Sub

Publier les événements dans Pub/Sub. Documenter les paramètres utiles à la démonstration, les droits d’accès et le comportement en cas d’erreur.

### 4. Traitement avec Dataflow

Construire le traitement streaming pour :

- lire les messages ;
- vérifier le schéma et les champs obligatoires ;
- transformer les données valides ;
- séparer ou signaler les événements invalides ;
- suivre les volumes reçus, traités et rejetés.

### 5. Stockage BigQuery

Écrire les événements traités dans BigQuery. Garder une sortie distincte ou un mécanisme de suivi pour diagnostiquer les événements rejetés.

### 6. Tests et analyses

Vérifier les volumes, les erreurs, les délais et les résultats produits. Ajouter des modèles dbt Fusion locaux si les transformations analytiques nécessitent une étape SQL supplémentaire.

---

## 📊 Périmètre des données et analyses

| Indicateur | Utilisation |
|---|---|
| Événements reçus par période | Suivre le débit d’ingestion |
| Événements traités par type | Comprendre le contenu du flux |
| Événements invalides ou rejetés | Mesurer les problèmes de qualité |
| Délai entre création et traitement | Estimer la latence si les horodatages le permettent |
| Événements dupliqués | Observer le comportement face aux doublons |

Les analyses doivent rester techniques : les événements sont générés artificiellement et ne représentent pas une activité médicale réelle.

---

## 🧹 Règles de qualité et de fiabilité

| Règle | Application |
|---|---|
| **Schéma** | Vérifier les champs obligatoires et leurs types |
| **Événements invalides** | Les isoler ou les signaler sans interrompre inutilement tout le flux |
| **Doublons** | Documenter l’identifiant d’événement et la stratégie choisie |
| **Reprise** | Tester le comportement après un échec de traitement |
| **Suivi des volumes** | Comparer les événements reçus, traités et rejetés |
| **Horodatages** | Distinguer l’heure de création et l’heure de traitement si disponibles |
| **Données** | Utiliser uniquement des événements synthétiques |

---

## 🔐 Sécurité, supervision et coûts

Documenter :

- les identités autorisées à publier et lire les messages ;
- les accès nécessaires pour Dataflow et BigQuery ;
- les moyens de consulter l’état des traitements et les erreurs ;
- les volumes et le rythme d’événements utilisés pour la démonstration ;
- les ressources à arrêter ou supprimer à la fin des essais ;
- les limites d’une architecture de démonstration par rapport à une architecture de production.

Ne versionne aucun secret ou identifiant sensible.

---

## 🧱 dbt Fusion — optionnel

Dans la première version, Dataflow peut écrire directement les événements traités dans BigQuery. dbt Fusion peut être ajouté ensuite pour :

- construire des modèles analytiques à partir des tables d’événements ;
- agréger les événements par période ou par type ;
- tester les identifiants, valeurs obligatoires et relations ;
- documenter les modèles.

Comme pour le projet batch, dbt Fusion peut être exécuté localement et se connecter à BigQuery. Il ne sera pas lancé automatiquement par le pipeline Dataflow sans étape d’orchestration supplémentaire.

---

## 📁 Structure du dépôt

```text
02-synthetic-health-streaming-pipeline/
├── README.md
├── .gitignore
├── .env.example
├── generator/
│   └── events/
├── pubsub/
│   └── docs/
├── dataflow/
│   ├── pipeline/
│   └── tests/
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   └── tests/
├── docs/
│   ├── architecture.md
│   ├── event_schema.md
│   └── reliability_and_costs.md
└── tests/
```

---

## ✅ Checklist de livraison

- [ ] Schéma des événements défini et documenté.
- [ ] Générateur d’événements synthétiques fonctionnel.
- [ ] Événements publiés dans Pub/Sub.
- [ ] Traitement Dataflow validé sur des événements valides et invalides.
- [ ] Événements traités consultables dans BigQuery.
- [ ] Volumes reçus, traités et rejetés comparés.
- [ ] Reprise et traitement des erreurs testés.
- [ ] Sécurité, supervision et hypothèses de coût documentées.
- [ ] Secrets absents du dépôt.
- [ ] README et schéma d’architecture ajoutés.

---

## 📌 Positionnement du projet

Ce projet montre que tu sais concevoir une chaîne streaming de démonstration, définir un schéma d’événements, ingérer avec Pub/Sub, traiter avec Dataflow et rendre les résultats accessibles dans BigQuery.

Il complète le projet Synthea batch en illustrant les différences entre **traitement de fichiers** et **traitement continu d’événements**.
