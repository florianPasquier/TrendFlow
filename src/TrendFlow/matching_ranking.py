from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np


#Nous pouvons utiliser le TF-IDF ou Word2Vec 
# pour comparer les tendances extraites (mots-clés, hashtags)
# avec les descriptions des produits.
# Voici un exemple d'utilisation de TF-IDF pour comparer les descriptions 
# de produits avec des mots-clés tendances :

# Liste de descriptions de produits fournisseurs (exemple)
products = [
    'Chaussures en cuir pour homme',
    'Vêtements d’été pour femme',
    'Montre connectée avec GPS',
    'Cosmétique anti-âge',
    'Sac à main en cuir tendance'
]

# Liste des tendances détectées (de Google Trends ou TikTok)
trends = [
    'chaussures tendances',
    'vêtements été',
    'montre smart',
    'soins de la peau',
    'sacs à main mode'
]

# Appliquer TF-IDF pour comparer
vectorizer = TfidfVectorizer()

# Combine les produits et les tendances pour le calcul de similarité
corpus = products + trends
X = vectorizer.fit_transform(corpus)

# Calculer la similarité entre chaque produit et chaque tendance
cosine_similarities = np.dot(X[:len(products)], X[len(products):].T).toarray()

# Afficher les similarités
for i, product in enumerate(products):
    print(f"Produit: {product}")
    for j, trend in enumerate(trends):
        print(f"  Tendance: {trend} - Similarité: {cosine_similarities[i][j]:.2f}")


# Une fois que nous avons calculé les similarités entre chaque produit 
# et chaque tendance, nous pouvons établir un classement des produits 
# en fonction de leur affinité avec les tendances populaires.
# 
# Calculer un score de chaque produit en fonction de sa similarité avec toutes les tendances
scores = cosine_similarities.max(axis=1)

# Créer un DataFrame pour voir le classement
ranked_products = pd.DataFrame({
    'Produit': products,
    'Score': scores
})

# Trier les produits en fonction du score
ranked_products = ranked_products.sort_values(by='Score', ascending=False)

# Afficher les produits classés
print(ranked_products)
