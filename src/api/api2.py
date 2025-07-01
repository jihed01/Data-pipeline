from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
from datetime import datetime, date
from typing import Optional, List
import uvicorn

app = FastAPI(
    title="Traffic Analytics API",
    description="API pour l'analyse du trafic en magasin",
    version="1.0.0"
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Chemin vers les données
DATA_PATH = "/home/ubuntu/Data-pipeline/data/filtered/daily_traffic_anomalies.parquet"

def load_data():
    """Charge les données depuis le fichier Parquet"""
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=404, detail="Fichier de données non trouvé")
    return pd.read_parquet(DATA_PATH)

@app.get("/")
async def root():
    """Point d'entrée de l'API"""
    return {
        "message": "API d'Analyse du Trafic en Magasin",
        "version": "1.0.0",
        "endpoints": [
            "/stores",
            "/api/traffic/{store_id}",
            "/api/sensors/{store_id}",
            "/api/anomalies",
            "/api/metrics/{store_id}/{sensor_id}"
        ]
    }

@app.get("/stores")
async def get_stores():
    """Récupère la liste de tous les magasins"""
    try:
        df = load_data()
        stores = df['id_du_magasin'].unique().tolist()
        return {
            "stores": stores,
            "count": len(stores)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/traffic/{store_id}")
async def get_store_traffic(
    store_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Récupère les données de trafic pour un magasin"""
    try:
        df = load_data()
        
        # Filtrage par magasin
        store_data = df[df['id_du_magasin'] == store_id]
        
        if store_data.empty:
            raise HTTPException(status_code=404, detail=f"Magasin {store_id} non trouvé")
        
        # Filtrage par dates si spécifiées
        if start_date:
            store_data = store_data[store_data['date'] >= pd.to_datetime(start_date)]
        if end_date:
            store_data = store_data[store_data['date'] <= pd.to_datetime(end_date)]
        
        # Conversion en format JSON
        result = store_data.to_dict('records')
        
        # Conversion des dates en string pour JSON
        for record in result:
            record['date'] = record['date'].strftime('%Y-%m-%d')
        
        return {
            "store_id": store_id,
            "data_count": len(result),
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sensors/{store_id}")
async def get_store_sensors(store_id: str):
    """Récupère la liste des capteurs pour un magasin"""
    try:
        df = load_data()
        
        store_data = df[df['id_du_magasin'] == store_id]
        
        if store_data.empty:
            raise HTTPException(status_code=404, detail=f"Magasin {store_id} non trouvé")
        
        sensors = store_data['id_du_capteur'].unique().tolist()
        
        return {
            "store_id": store_id,
            "sensors": sensors,
            "count": len(sensors)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/anomalies")
async def get_anomalies(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    store_id: Optional[str] = None,
    threshold: Optional[float] = 50.0
):
    """Récupère les anomalies détectées sur une période"""
    try:
        df = load_data()
        
        # Filtrage des anomalies
        anomalies = df[abs(df['pct_change']) > threshold]
        
        # Filtrage par magasin si spécifié
        if store_id:
            anomalies = anomalies[anomalies['id_du_magasin'] == store_id]
        
        # Filtrage par dates si spécifiées
        if start_date:
            anomalies = anomalies[anomalies['date'] >= pd.to_datetime(start_date)]
        if end_date:
            anomalies = anomalies[anomalies['date'] <= pd.to_datetime(end_date)]
        
        # Conversion en format JSON
        result = anomalies.to_dict('records')
        
        # Conversion des dates en string pour JSON
        for record in result:
            record['date'] = record['date'].strftime('%Y-%m-%d')
        
        return {
            "anomalies_count": len(result),
            "threshold": threshold,
            "anomalies": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/{store_id}/{sensor_id}")
async def get_sensor_metrics(store_id: str, sensor_id: int):
    """Métriques détaillées pour un capteur spécifique"""
    try:
        df = load_data()
        
        # Filtrage par magasin et capteur
        sensor_data = df[
            (df['id_du_magasin'] == store_id) & 
            (df['id_du_capteur'] == sensor_id)
        ]
        
        if sensor_data.empty:
            raise HTTPException(
                status_code=404, 
                detail=f"Capteur {sensor_id} non trouvé pour le magasin {store_id}"
            )
        
        # Calcul des métriques
        metrics = {
            "store_id": store_id,
            "sensor_id": sensor_id,
            "data_points": len(sensor_data),
            "date_range": {
                "start": sensor_data['date'].min().strftime('%Y-%m-%d'),
                "end": sensor_data['date'].max().strftime('%Y-%m-%d')
            },
            "traffic_metrics": {
                "mean": float(sensor_data['trafic_journalier'].mean()),
                "median": float(sensor_data['trafic_journalier'].median()),
                "min": int(sensor_data['trafic_journalier'].min()),
                "max": int(sensor_data['trafic_journalier'].max()),
                "std": float(sensor_data['trafic_journalier'].std())
            },
            "anomalies": {
                "count": len(sensor_data[abs(sensor_data['pct_change']) > 50]),
                "percentage": float(len(sensor_data[abs(sensor_data['pct_change']) > 50]) / len(sensor_data) * 100)
            },
            "weekly_pattern": sensor_data.groupby('jour_semaine')['trafic_journalier'].mean().to_dict()
        }
        
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health_check():
    """Vérification de l'état de l'API"""
    try:
        df = load_data()
        return {
            "status": "healthy",
            "data_available": True,
            "total_records": len(df),
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "data_available": False,
            "error": str(e),
            "last_updated": datetime.now().isoformat()
        }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

