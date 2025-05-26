#pylint : disable = missing-module-docstring
import csv
import os
import random
from datetime import date, timedelta
from typing import Optional

import requests

# Configuration
BASE_API_URL = "http://127.0.0.1:8000/"
STORES = ["Lille", "Paris", "Lyon", "Toulouse", "Marseille"]
OUTPUT_DIR = "data/raw"
SENSORS = list(range(8))  # IDs 0 à 7


def fetch_store_data(
    store_name: str, query_date: date, sensor_id: Optional[int] = None
) -> Optional[float]:
    """
    Récupère les données de visiteurs d'un magasin via l'API.

    Args:
        store_name: Nom du magasin (ex: "Lille")
        query_date: Date pour laquelle récupérer les données
        sensor_id: ID du capteur (0-7). Si None, retourne le total du magasin

    Returns:
        float: Nombre de visiteurs
        None: En cas d'erreur
        -1: Si le magasin est fermé (dimanche)

    Raises:
        Affiche les erreurs de requête mais ne les propage pas
    """
    if query_date.weekday() == 6:  # Dimanche
        return -1

    params = {
        "store_name": store_name,
        "year": query_date.year,
        "month": query_date.month,
        "day": query_date.day,
    }
    if sensor_id is not None:
        params["sensor_id"] = sensor_id

    try:
        response = requests.get(BASE_API_URL, params=params, timeout=5)
        response.raise_for_status()
        return float(response.json())
    except Exception as e:
        print(f"Erreur pour {store_name} le {query_date}: {str(e)}")
        return None


def generate_monthly_report(
    start_date: date, end_date: date, add_noise: bool = False, noise_rate: float = 0.1
) -> None:
    """
    Génère des rapports mensuels au format CSV contenant les données de visiteurs.

    Args:
        start_date: Date de début (incluse)
        end_date: Date de fin (incluse)
        add_noise: Si True, ajoute des données corrompues
        noise_rate: Probabilité d'ajouter une ligne corrompue (0.1 = 10%)

    Returns:
        None: Les fichiers sont écrits dans le dossier OUTPUT_DIR

    Notes:
        - Format des fichiers: visiteurs_YYYY-MM.csv
        - Structure des données:
            date, heure, id_du_capteur, id_du_magasin, nombre_visiteurs, unite
        - Données corrompues possibles:
            - Capteur: None ou 999
            - Visiteurs: -1 ou 999999
            - Unité: valeurs aléatoires ("litres", "kg", etc.)
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    current_month = start_date.replace(day=1)
    while current_month <= end_date:
        month_str = current_month.strftime("%Y-%m")
        filename = os.path.join(OUTPUT_DIR, f"visiteurs_{month_str}.csv")

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "date",
                    "heure",
                    "id_du_capteur",
                    "id_du_magasin",
                    "nombre_visiteurs",
                    "unite",
                ]
            )

            day = current_month.replace(day=1)
            while day.month == current_month.month and day <= end_date:
                for store in STORES:
                    for sensor in SENSORS:
                        visitors = fetch_store_data(store, day, sensor)
                        if visitors is not None:
                            writer.writerow(
                                [
                                    day.strftime("%Y-%m-%d"),
                                    "12:00:00",
                                    sensor,
                                    store,
                                    visitors,
                                    "visiteurs",
                                ]
                            )

                    if add_noise and random.random() < noise_rate:
                        writer.writerow(
                            [
                                day.strftime("%Y-%m-%d"),
                                "12:00:00",
                                None if random.random() < 0.5 else 999,
                                store,
                                -1 if random.random() < 0.5 else 999999,
                                random.choice(["litres", "kg", "foo"]),
                            ]
                        )
                day += timedelta(days=1)
        print(f"Fichier généré : {filename}")
        current_month = (current_month.replace(day=28) + timedelta(days=4)).replace(
            day=1
        )


if __name__ == "__main__":
    generate_monthly_report(
        start_date=date(2025, 1, 1),
        end_date=date.today(),
        add_noise=True,
        noise_rate=0.1,
    )
