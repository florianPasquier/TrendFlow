# ✨ Solution de Prédiction des Tendances Produits pour l'E-commerce

## 📊 Introduction
Ce projet a pour objectif d'identifier les produits à fort potentiel en analysant les tendances de vente historiques, les tendances issues des réseaux sociaux (Google Trends, TikTok) et les catalogues de nouveaux produits.

L'architecture repose sur une collecte de données orchestrée avec **Apache Airflow**, un stockage et une transformation des données via **BigQuery et DBT**, un **modèle de Machine Learning avec Vertex AI ou Spark ML**, et une **exposition des prédictions via API FastAPI et un dashboard Streamlit**.

---

## 🔍 Architecture Technique

1. **Orchestration** : Apache Airflow planifie et exécute les pipelines de collecte et transformation des données.
2. **Stockage** : Les données sources (ventes, tendances, nouveaux produits) sont stockées dans **Google Cloud Storage (GCS)**.
3. **Transformation & Unification** : DBT structure les données et les agrège dans **BigQuery** pour créer trois datasets :
   - `sales_history`
   - `trend`
   - `new_product`
4. **Entraînement du Modèle** : Utilisation de **Vertex AI AutoML** ou **Spark ML sur Dataproc** pour prédire les meilleures opportunités de produits.
5. **API REST** : FastAPI expose les recommandations de produits basées sur les tendances.
6. **Dashboard** : Streamlit permet de visualiser les tendances et recommandations.
7. **Déploiement & CI/CD** : Docker, Cloud Run et GitHub Actions assurent l'automatisation et le déploiement des services.

---

## 🚿 Flux de Données
1. **Récupération des données** (ventes, tendances, nouveaux produits) via API ou fichiers CSV.
2. **Stockage dans Google Cloud Storage**.
3. **Transformation et nettoyage avec DBT et ingestion dans BigQuery**.
4. **Modélisation Machine Learning pour la prédiction des ventes**.
5. **Stockage des prédictions dans BigQuery**.
6. **Exposition des résultats via une API REST**.
7. **Affichage des tendances et recommandations sur le dashboard Streamlit**.

## 🛠️ Technologies Utilisées
- **Orchestration** : Apache Airflow
- **Stockage** : Google Cloud Storage (GCS)
- **Transformation** : DBT + BigQuery
- **Machine Learning** : Google Vertex AI / Spark ML (Dataproc)
- **API REST** : FastAPI
- **Interface utilisateur** : Streamlit
- **Déploiement** : Docker, Cloud Run, GitHub Actions

## 🌐 Déploiement & Exécution
### TrendFlow
#### Setup
#### Terraform
```
cd terraform/
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```
### Docker (local config if need)
```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up --build   
```
### 1. **Installation des dépendances**
```bash
pip install -r requirements.txt
```

### 2. **Exécution des pipelines Airflow**
```bash
airflow scheduler & airflow webserver -p 8080
```

### 3. **Lancer l'API**
```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

### 4. **Démarrer l'interface utilisateur**
```bash
streamlit run frontend/app.py
```

---

## 🏷️ Conclusion
Ce projet fournit une solution complète et automatisée pour l'analyse des tendances et la prédiction des produits prometteurs dans l'e-commerce. Il est conçu pour être scalable, flexible et intégrable avec d'autres outils d'analyse de marché.

🚀 **Prêt à être mis en production !**