import streamlit as st
import requests

st.title("🛒 Produits Tendances")

data = requests.get("https://trend-api.run.app/recommendations").json()

for product in data:
    st.write(f"**Produit :** {product['product_id']} | **Score :** {product['prediction']}")
