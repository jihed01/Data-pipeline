import os
import pandas as pd
from pathlib import Path
from typing import List
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

raw_data_path = "./data/raw"

def get_visitor_csv_files(raw_data_path: str) -> List[Path]:
    raw_dir = Path(raw_data_path)
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"Directory not found: {raw_dir}")
    csv_files = list(raw_dir.glob("visiteurs_*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files matching pattern 'visiteurs_*.csv' found in {raw_dir}")
    return csv_files

def load_and_merge_data(raw_data_path: str) -> pd.DataFrame:
    csv_files = get_visitor_csv_files(raw_data_path)
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as e:
             print(f"Error loading {file.name}: {str(e)}")
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df["date"] = pd.to_datetime(combined_df["date"])
    combined_df.sort_values(["date", "id_du_capteur", "id_du_magasin"], inplace=True)
    return combined_df

# Step 1: Lecture et Fusion des Données CSV
try:
    df_combined = load_and_merge_data(raw_data_path)
    print(f"Nombre de lignes: {df_combined.shape[0]}")
    print(f"Nombre de colonnes: {df_combined.shape[1]}")
    print("Premières lignes du DataFrame fusionné:")
    print(df_combined.head())
except Exception as e:
    print(f"Erreur lors de l'ingestion des données: {e}")

# Step 2: Agrégation Journalisée
df_combined["heure"] = pd.to_datetime(df_combined["heure"]).dt.time
df_combined["id_du_capteur"] = df_combined["id_du_capteur"].fillna(-1).astype(int) # Handle missing sensor IDs
df_combined["nombre_visiteurs"] = pd.to_numeric(df_combined["nombre_visiteurs"], errors='coerce').fillna(0) # Handle non-numeric and NaN values
df_combined["nombre_visiteurs"] = df_combined["nombre_visiteurs"].apply(lambda x: max(0, x)) # Ensure no negative visitors

df_daily_traffic = df_combined.groupby(["date", "id_du_magasin", "id_du_capteur"])["nombre_visiteurs"].sum().reset_index()
df_daily_traffic.rename(columns={"nombre_visiteurs": "trafic_journalier"}, inplace=True)

df_daily_traffic["jour_semaine"] = df_daily_traffic["date"].dt.day_name()
df_daily_traffic["mois"] = df_daily_traffic["date"].dt.month_name()
df_daily_traffic["annee"] = df_daily_traffic["date"].dt.year

print("\nAgrégation Journalisée (premières lignes):")
print(df_daily_traffic.head())

# Step 3: Calcul de la Moyenne Mobile sur 4 Semaines
def calculate_rolling_average(df: pd.DataFrame) -> pd.DataFrame:
    df_sorted = df.sort_values(by="date")
    df_sorted["moyenne_mobile_4_semaines"] = df_sorted.groupby(["id_du_magasin", "id_du_capteur", "jour_semaine"])["trafic_journalier"].transform(
        lambda x: x.rolling(window=4, min_periods=1).mean()
    )
    return df_sorted

df_daily_traffic = calculate_rolling_average(df_daily_traffic)
print("\nMoyenne Mobile sur 4 Semaines (premières lignes):")
print(df_daily_traffic.head())

# Step 4: Détection d’Anomalies par Écart Relatif
def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    df["pct_change"] = np.where(
        df["moyenne_mobile_4_semaines"] == 0,
        np.nan, # Avoid division by zero, set to NaN
        ((df["trafic_journalier"] - df["moyenne_mobile_4_semaines"]) / df["moyenne_mobile_4_semaines"]) * 100
    )
    # Cap the percentage change to a reasonable range for display/analysis
    df["pct_change"] = df["pct_change"].clip(lower=-100, upper=200) # Example range, adjust as needed
    return df

df_daily_traffic = detect_anomalies(df_daily_traffic)
print("\nDétection d'Anomalies (premières lignes avec pct_change):")
print(df_daily_traffic.head())

# Step 5: Exportation au Format Parquet
output_dir = "/home/ubuntu/Data-pipeline/data/filtered"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "daily_traffic_anomalies.parquet")
df_daily_traffic.to_parquet(output_file, index=False)
print(f"Données filtrées et nettoyées exportées vers: {output_file}")
print("\nAvantages du format Parquet par rapport à CSV:")
print("- **Compression**: Parquet utilise des schémas de compression avancés, réduisant considérablement la taille des fichiers.")
print("- **Performance**: C'est un format de stockage colonnaire, ce qui signifie que les requêtes qui ne nécessitent que certaines colonnes sont beaucoup plus rapides car elles ne lisent pas les données non pertinentes.")
print("- **Schéma évolutif**: Parquet prend en charge l'évolution du schéma, ce qui est utile dans les pipelines de données où les schémas peuvent changer au fil du temps.")
print("- **Interopérabilité**: Largement utilisé dans l'écosystème Big Data (Spark, Hive, Impala, etc.).")

