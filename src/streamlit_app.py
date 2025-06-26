import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import os

# Configuration de la page
st.set_page_config(
    page_title="Analyse du Trafic en Magasin",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Titre principal
st.title("📊 Analyse du Trafic en Magasin")
st.markdown("---")

# Fonction pour charger les données
@st.cache_data
def load_data():
    """Charge les données depuis le fichier Parquet"""
    data_path = "/home/ubuntu/Data-pipeline/data/filtered/daily_traffic_anomalies.parquet"
    if os.path.exists(data_path):
        return pd.read_parquet(data_path)
    else:
        st.error(f"Fichier de données non trouvé: {data_path}")
        return None

# Chargement des données
df = load_data()

if df is not None:
    # Sidebar pour les filtres
    st.sidebar.header("🔍 Filtres")
    
    # Sélection du magasin
    magasins = sorted(df['id_du_magasin'].unique())
    selected_magasin = st.sidebar.selectbox(
        "Sélectionnez un magasin:",
        magasins,
        index=0
    )
    
    # Filtrage par magasin
    df_magasin = df[df['id_du_magasin'] == selected_magasin]
    
    # Sélection du capteur
    capteurs = sorted(df_magasin['id_du_capteur'].unique())
    selected_capteur = st.sidebar.selectbox(
        "Sélectionnez un capteur:",
        capteurs,
        index=0
    )
    
    # Filtrage par capteur
    df_filtered = df_magasin[df_magasin['id_du_capteur'] == selected_capteur]
    
    # Sélection de la granularité
    granularite = st.sidebar.radio(
        "Granularité d'affichage:",
        ["Semaine", "Mois"],
        index=0
    )
    
    # Affichage des informations sélectionnées
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Magasin sélectionné:** {selected_magasin}")
    st.sidebar.markdown(f"**Capteur sélectionné:** {selected_capteur}")
    st.sidebar.markdown(f"**Nombre de points de données:** {len(df_filtered)}")
    
    # Section principale
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header(f"📈 Données pour {selected_magasin} - Capteur {selected_capteur}")
        
        # Affichage du DataFrame filtré
        st.subheader("📋 Données filtrées")
        st.dataframe(
            df_filtered[['date', 'trafic_journalier', 'moyenne_mobile_4_semaines', 'pct_change', 'jour_semaine']],
            use_container_width=True
        )
    
    with col2:
        st.header("📊 Statistiques")
        
        # Métriques
        avg_traffic = df_filtered['trafic_journalier'].mean()
        max_traffic = df_filtered['trafic_journalier'].max()
        min_traffic = df_filtered['trafic_journalier'].min()
        
        st.metric("Trafic moyen", f"{avg_traffic:.0f}")
        st.metric("Trafic maximum", f"{max_traffic:.0f}")
        st.metric("Trafic minimum", f"{min_traffic:.0f}")
        
        # Anomalies détectées
        anomalies = df_filtered[abs(df_filtered['pct_change']) > 50]
        st.metric("Anomalies détectées", len(anomalies))
    
    # Visualisations
    st.markdown("---")
    st.header("📊 Visualisations")
    
    # Préparation des données selon la granularité
    if granularite == "Semaine":
        df_viz = df_filtered.copy()
        df_viz['periode'] = df_viz['date'].dt.to_period('W').astype(str)
        df_agg = df_viz.groupby('periode').agg({
            'trafic_journalier': 'sum',
            'moyenne_mobile_4_semaines': 'mean',
            'date': 'first'
        }).reset_index()
        titre_granularite = "Hebdomadaire"
    else:  # Mois
        df_viz = df_filtered.copy()
        df_viz['periode'] = df_viz['date'].dt.to_period('M').astype(str)
        df_agg = df_viz.groupby('periode').agg({
            'trafic_journalier': 'sum',
            'moyenne_mobile_4_semaines': 'mean',
            'date': 'first'
        }).reset_index()
        titre_granularite = "Mensuelle"
    
    # Graphique 1: Trafic journalier
    fig1 = go.Figure()
    
    fig1.add_trace(go.Scatter(
        x=df_filtered['date'],
        y=df_filtered['trafic_journalier'],
        mode='lines+markers',
        name='Trafic journalier',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=6)
    ))
    
    fig1.update_layout(
        title=f"Évolution du Trafic Journalier - {selected_magasin} (Capteur {selected_capteur})",
        xaxis_title="Date",
        yaxis_title="Nombre de visiteurs",
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    st.plotly_chart(fig1, use_container_width=True)
    
    # Graphique 2: Comparaison avec la moyenne mobile
    fig2 = go.Figure()
    
    fig2.add_trace(go.Scatter(
        x=df_filtered['date'],
        y=df_filtered['trafic_journalier'],
        mode='lines+markers',
        name='Trafic journalier',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=4)
    ))
    
    fig2.add_trace(go.Scatter(
        x=df_filtered['date'],
        y=df_filtered['moyenne_mobile_4_semaines'],
        mode='lines',
        name='Moyenne mobile 4 semaines',
        line=dict(color='#ff7f0e', width=3, dash='dash')
    ))
    
    fig2.update_layout(
        title=f"Comparaison Trafic vs Moyenne Mobile - {selected_magasin} (Capteur {selected_capteur})",
        xaxis_title="Date",
        yaxis_title="Nombre de visiteurs",
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    st.plotly_chart(fig2, use_container_width=True)
    
    # Graphique 3: Détection d'anomalies
    fig3 = go.Figure()
    
    # Points normaux
    normal_points = df_filtered[abs(df_filtered['pct_change']) <= 50]
    fig3.add_trace(go.Scatter(
        x=normal_points['date'],
        y=normal_points['pct_change'],
        mode='markers',
        name='Variation normale',
        marker=dict(color='green', size=6),
        text=normal_points['trafic_journalier'],
        hovertemplate='<b>Date:</b> %{x}<br><b>Variation:</b> %{y:.1f}%<br><b>Trafic:</b> %{text}<extra></extra>'
    ))
    
    # Anomalies
    anomaly_points = df_filtered[abs(df_filtered['pct_change']) > 50]
    if len(anomaly_points) > 0:
        fig3.add_trace(go.Scatter(
            x=anomaly_points['date'],
            y=anomaly_points['pct_change'],
            mode='markers',
            name='Anomalies détectées',
            marker=dict(color='red', size=10, symbol='diamond'),
            text=anomaly_points['trafic_journalier'],
            hovertemplate='<b>Date:</b> %{x}<br><b>Variation:</b> %{y:.1f}%<br><b>Trafic:</b> %{text}<extra></extra>'
        ))
    
    # Lignes de seuil
    fig3.add_hline(y=50, line_dash="dash", line_color="red", opacity=0.5)
    fig3.add_hline(y=-50, line_dash="dash", line_color="red", opacity=0.5)
    
    fig3.update_layout(
        title=f"Détection d'Anomalies - {selected_magasin} (Capteur {selected_capteur})",
        xaxis_title="Date",
        yaxis_title="Variation par rapport à la moyenne mobile (%)",
        hovermode='closest',
        template='plotly_white',
        height=400
    )
    
    st.plotly_chart(fig3, use_container_width=True)
    
    # Graphique 4: Analyse par jour de la semaine
    st.markdown("---")
    st.header("📅 Analyse par Jour de la Semaine")
    
    # Ordre des jours de la semaine
    jours_ordre = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df_jours = df_filtered.groupby('jour_semaine')['trafic_journalier'].agg(['mean', 'std']).reset_index()
    df_jours['jour_semaine'] = pd.Categorical(df_jours['jour_semaine'], categories=jours_ordre, ordered=True)
    df_jours = df_jours.sort_values('jour_semaine')
    
    fig4 = go.Figure()
    
    fig4.add_trace(go.Bar(
        x=df_jours['jour_semaine'],
        y=df_jours['mean'],
        error_y=dict(type='data', array=df_jours['std']),
        name='Trafic moyen',
        marker_color='lightblue'
    ))
    
    fig4.update_layout(
        title=f"Trafic Moyen par Jour de la Semaine - {selected_magasin} (Capteur {selected_capteur})",
        xaxis_title="Jour de la semaine",
        yaxis_title="Trafic moyen",
        template='plotly_white',
        height=400
    )
    
    st.plotly_chart(fig4, use_container_width=True)
    
    # Informations supplémentaires
    st.markdown("---")
    st.header("ℹ️ Informations sur les Données")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"""
        **Période couverte:**
        Du {df_filtered['date'].min().strftime('%d/%m/%Y')} 
        au {df_filtered['date'].max().strftime('%d/%m/%Y')}
        """)
    
    with col2:
        st.info(f"""
        **Magasins disponibles:**
        {', '.join(magasins)}
        """)
    
    with col3:
        st.info(f"""
        **Capteurs pour {selected_magasin}:**
        {', '.join(map(str, capteurs))}
        """)

else:
    st.error("Impossible de charger les données. Veuillez vérifier que le fichier de données existe.")
    st.info("Exécutez d'abord le script de traitement des données pour générer le fichier Parquet.")

