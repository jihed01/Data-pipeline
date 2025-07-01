# 📊 Pipeline de Données pour l'Analyse du Trafic en Magasin

## 🎯 Vue d'ensemble

Ce projet implémente une pipeline de données complète pour l'analyse en temps réel du trafic de visiteurs dans les magasins de détail. Le système automatise l'ingestion, la transformation, la détection d'anomalies et la visualisation des données de capteurs de trafic.

## 🏗️ Architecture du Système

### Technologies Utilisées

- **🐍 Python** : Langage principal pour le traitement des données
- **🗄️ SQL** : Requêtes et manipulation de données
- **🐼 Pandas** : Manipulation et analyse de données
- **⚡ FastAPI** : API backend pour l'exposition des données
- **🌊 Airflow** : Orchestration et planification des tâches
- **📊 Streamlit** : Interface utilisateur interactive
- **📈 Plotly** : Visualisations interactives
- **🗂️ Parquet** : Format de stockage optimisé

### Flux de Données

```
📁 Données Brutes (CSV)
    ↓
🔄 Ingestion & Fusion (Python/Pandas)
    ↓
🧮 Agrégation Journalière
    ↓
📊 Calcul Moyenne Mobile (4 semaines)
    ↓
🚨 Détection d'Anomalies
    ↓
💾 Stockage (Parquet)
    ↓
🌐 API FastAPI
    ↓
📱 Interface Streamlit + Plotly
```

## 📂 Structure du Projet

```
Data-pipeline/
├── data/
│   ├── raw/                    # Fichiers CSV bruts
│   └── filtered/               # Données traitées (Parquet)
├── src/
│   ├── data_processing.py      # Script principal de traitement
│   ├── streamlit_app.py        # Application Streamlit
│   └── DataTransformation.ipynb # Notebook d'analyse
├── requirements.txt            # Dépendances Python
└── README.md                   # Documentation
```

## 🚀 Installation et Configuration

### Prérequis

- Python 3.8+
- pip (gestionnaire de paquets Python)

### Installation des Dépendances

```bash
pip install pandas plotly streamlit pyarrow fastapi uvicorn
```

### Variables d'Environnement

Aucune configuration spéciale requise pour l'environnement local.

## 📋 Utilisation

### 1. Traitement des Données

Exécutez le script principal pour traiter les données brutes :

```bash
python src/data_processing.py
```

Ce script :
- Lit tous les fichiers `visiteurs_*.csv` du dossier `data/raw`
- Fusionne les données en un seul DataFrame
- Effectue l'agrégation journalière
- Calcule la moyenne mobile sur 4 semaines
- Détecte les anomalies (variations > ±50%)
- Exporte les résultats en format Parquet

### 2. Lancement de l'Interface Streamlit

```bash
streamlit run src/streamlit_app.py --server.port 8501 --server.address 0.0.0.0
```

L'application sera accessible à l'adresse : `http://localhost:8501`

### 3. Fonctionnalités de l'Interface

#### Filtres Interactifs
- **Sélection du magasin** : Choisissez parmi les magasins disponibles
- **Sélection du capteur** : Filtrez par capteur spécifique
- **Granularité** : Visualisation par semaine ou par mois

#### Visualisations Disponibles
1. **Évolution du Trafic Journalier** : Courbe temporelle du trafic
2. **Comparaison avec Moyenne Mobile** : Trafic vs moyenne des 4 dernières semaines
3. **Détection d'Anomalies** : Points anormaux mis en évidence
4. **Analyse par Jour de la Semaine** : Patterns hebdomadaires

#### Métriques Clés
- Trafic moyen, maximum et minimum
- Nombre d'anomalies détectées
- Statistiques par période

## 🔧 Détails Techniques

### Algorithme de Détection d'Anomalies

```python
# Calcul de la variation relative
pct_change = (valeur_actuelle - moyenne_mobile) / moyenne_mobile * 100

# Seuil de détection : ±50%
anomalie = abs(pct_change) > 50
```

### Format de Données

#### Données d'Entrée (CSV)
```csvdétail
date,heure,id_du_magasin,id_du_capteur,nombre_de_visiteurs
2025-02-04,00:00:00,Lille,3,125  # Exemple avec un ID de capteur valide (0-7)
```

#### Données de Sortie (Parquet)
```python
{
    'date': datetime,
    'id_du_magasin': str,
    'id_du_capteur': int,          # Valeurs normalisées entre 0-7
    'trafic_journalier': int,
    'moyenne_mobile_4_semaines': float,
    'pct_change': float,
    'jour_semaine': str
}
```

### Avantages du Format Parquet

- **Performance** : Lecture/écriture 10x plus rapide que CSV
- **Compression** : Réduction de 70-90% de la taille des fichiers
- **Typage** : Conservation des types de données
- **Compatibilité** : Support natif par Pandas, Spark, etc.

## 🔄 Orchestration avec Airflow

### DAG Principal

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    'traffic_analysis_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1)
)

# Tâches
ingest_data = PythonOperator(
    task_id='ingest_csv_files',
    python_callable=load_and_merge_data
)

process_data = PythonOperator(
    task_id='process_and_aggregate',
    python_callable=calculate_metrics
)

detect_anomalies = PythonOperator(
    task_id='detect_anomalies',
    python_callable=anomaly_detection
)

# Dépendances
ingest_data >> process_data >> detect_anomalies
```

## 🌐 API FastAPI

### Endpoints Disponibles

```python
@app.get("/api/traffic/{store_id}")
async def get_store_traffic(store_id: str):
    """Récupère les données de trafic pour un magasin"""

@app.get("/api/anomalies")
async def get_anomalies(start_date: str, end_date: str):
    """Récupère les anomalies détectées sur une période"""

@app.get("/api/metrics/{store_id}/{sensor_id}")
async def get_sensor_metrics(store_id: str, sensor_id: int):
    """Métriques détaillées pour un capteur spécifique"""
```

### Lancement de l'API

```bash
uvicorn src.api:app --host 0.0.0.0 --port 8000
```

## 📊 Cas d'Usage Métier

### 1. Surveillance en Temps Réel
- Détection automatique des pics de fréquentation
- Alertes en cas d'anomalies de trafic
- Monitoring des performances par magasin

### 2. Analyse Prédictive
- Identification des patterns saisonniers
- Prévision des périodes de forte affluence
- Optimisation des ressources humaines

### 3. Reporting Exécutif
- Tableaux de bord interactifs
- Métriques KPI automatisées
- Comparaisons inter-magasins

### 4. Optimisation Opérationnelle
- Analyse des jours de la semaine
- Identification des heures de pointe
- Planification des promotions

## 🧪 Tests et Validation

### Tests Unitaires

```bash
python -m pytest tests/
```

### Validation des Données

```python
# Vérification de la cohérence des données
assert df['trafic_journalier'].min() >= 0
assert df['pct_change'].between(-100, 200).all()
```

## 🚀 Déploiement

### Environnement Local

1. Cloner le repository
2. Installer les dépendances
3. Placer les fichiers CSV dans `data/raw/`
4. Exécuter le traitement des données
5. Lancer Streamlit

---------------
**Dernière mise à jour** :  1 juillet 2025  
**Version** : 1.0.0  
**Statut** : Production Ready ✅
