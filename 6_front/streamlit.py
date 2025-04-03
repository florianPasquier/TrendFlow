import streamlit as st
import requests
import pandas as pd

st.title("📊 Prédictions des ventes et recommandations")

# Récupérer les prévisions
response = requests.get("http://localhost:8000/predictions")
df_predictions = pd.DataFrame(response.json())

# Récupérer les recommandations
response = requests.get("http://localhost:8000/recommendations")
df_recommendations = pd.DataFrame(response.json())

# Afficher les prévisions sous forme de graphique
st.line_chart(df_predictions.set_index("ds")["yhat"])

# Afficher les recommandations
st.write("🚀 **Produits recommandés :**")
st.table(df_recommendations)
