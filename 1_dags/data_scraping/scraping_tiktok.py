# !pip install TikTok-Api

from TikTokApi import TikTokApi

# Initialisation de l'API TikTok
api = TikTokApi.get_instance()

# Exemple de récupération de tendances
trending = api.discover_hashtags()

# Afficher les tendances populaires
for tag in trending:
    print(f"Hashtag: {tag['challengeInfo']['challengeName']}, Popularity: {tag['challengeInfo']['stats']['totalShares']}")
