from pytrends.request import TrendReq
import pandas as pd

# Initialiser pytrends
pytrends = TrendReq(hl='fr', tz=360)

# Liste de mots-clés pour lesquels vous voulez analyser les tendances
keywords = ['produit électronique', 'vêtements', 'accessoires', 'chaussures', 'cosmétiques']

# Récupérer les tendances de recherche pour les mots-clés
pytrends.build_payload(keywords, cat=0, timeframe='today 12-m', geo='', gprop='')

# Obtenir l'intérêt au fil du temps
interest_over_time_df = pytrends.interest_over_time()

# Affichage des résultats
print(interest_over_time_df.head())

# Sauvegarder les résultats dans un fichier CSV
interest_over_time_df.to_csv('google_trends_data.csv')


# !pip install TikTok-Api

from TikTokApi import TikTokApi

# Initialisation de l'API TikTok
api = TikTokApi.get_instance()

# Exemple de récupération de tendances
trending = api.discover_hashtags()

# Afficher les tendances populaires
for tag in trending:
    print(f"Hashtag: {tag['challengeInfo']['challengeName']}, Popularity: {tag['challengeInfo']['stats']['totalShares']}")
