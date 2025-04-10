from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import pandas as pd # Added import
import logging # Added import

logger = logging.getLogger(__name__)

def generate_recommendations(products: list[str], trends: list[str]) -> pd.DataFrame:
    """
    Generates product recommendations based on TF-IDF similarity with trends.

    Args:
        products: A list of product descriptions (strings).
        trends: A list of trend keywords (strings).

    Returns:
        A pandas DataFrame with 'Produit' and 'Score' columns,
        sorted by score in descending order. Returns empty DataFrame if inputs are empty
        or if errors occur during processing.
    """
    if not products or not trends:
        logger.warning("generate_recommendations called with empty products or trends list.")
        return pd.DataFrame({'Produit': [], 'Score': []})

    logger.info(f"Generating recommendations for {len(products)} products and {len(trends)} trends.")
    vectorizer = TfidfVectorizer()

    # Combine products and trends for TF-IDF calculation
    corpus = products + trends
    try:
        X = vectorizer.fit_transform(corpus)
        logger.debug(f"TF-IDF matrix shape: {X.shape}")
    except ValueError as e:
        # Handle case where vocabulary might be empty (e.g., only stop words)
        logger.error(f"TF-IDF Vectorization failed: {e}. Returning zero scores.")
        return pd.DataFrame({'Produit': products, 'Score': [0.0] * len(products)})

    # Separate product and trend vectors
    num_products = len(products)
    num_trends = len(trends)

    # Basic dimension check after vectorization
    if X.shape[0] != num_products + num_trends:
         logger.error(f"Mismatch in TF-IDF matrix dimensions after fit_transform. Expected {num_products + num_trends}, got {X.shape[0]}.")
         # Decide how to handle this - returning empty might be safest
         return pd.DataFrame({'Produit': [], 'Score': []})

    product_vectors = X[:num_products]
    trend_vectors = X[num_products:]

    # Handle cases where one set might be empty after vectorization (e.g., all stop words)
    if product_vectors.shape[0] == 0 or trend_vectors.shape[0] == 0:
         logger.warning("Product or Trend vectors are empty after TF-IDF processing.")
         return pd.DataFrame({'Produit': products, 'Score': [0.0] * len(products)})

    # Calculate cosine similarities
    try:
        cosine_similarities = np.dot(product_vectors, trend_vectors.T).toarray()
        logger.debug(f"Cosine similarity matrix shape: {cosine_similarities.shape}")
    except Exception as e:
        logger.error(f"Error calculating cosine similarities: {e}")
        return pd.DataFrame({'Produit': [], 'Score': []})


    # Calculate score for each product based on max similarity with any trend
    if cosine_similarities.shape[1] == 0: # No trends to compare against
        logger.warning("Cosine similarity matrix has no columns (no trends?). Assigning zero scores.")
        scores = np.zeros(num_products)
    else:
        try:
            scores = cosine_similarities.max(axis=1)
        except Exception as e:
             logger.error(f"Error calculating max scores from similarities: {e}")
             return pd.DataFrame({'Produit': [], 'Score': []})


    # Create and rank DataFrame
    try:
        ranked_products = pd.DataFrame({
            'Produit': products, # Use original product list for names
            'Score': scores
        })
        ranked_products = ranked_products.sort_values(by='Score', ascending=False).reset_index(drop=True)
        logger.info(f"Successfully generated {len(ranked_products)} recommendations.")
        return ranked_products
    except Exception as e:
        logger.error(f"Error creating or sorting final DataFrame: {e}")
        return pd.DataFrame({'Produit': [], 'Score': []})


# --- Example Usage ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO) # Setup basic logging for example run
    # Example product descriptions
    example_products = [
        'Chaussures en cuir pour homme',
        'Vêtements d’été pour femme',
        'Montre connectée avec GPS',
        'Cosmétique anti-âge',
        'Sac à main en cuir tendance'
    ]

    # Example detected trends
    example_trends = [
        'chaussures tendances',
        'vêtements été',
        'montre smart',
        'soins de la peau',
        'sacs à main mode'
    ]

    # --- Original Example Code (TF-IDF calculation part for comparison) ---
    # vectorizer_ex = TfidfVectorizer()
    # corpus_ex = example_products + example_trends
    # X_ex = vectorizer_ex.fit_transform(corpus_ex)
    # cosine_similarities_ex = np.dot(X_ex[:len(example_products)], X_ex[len(example_products):].T).toarray()

    # # Display detailed similarities (original example output)
    # print("--- Detailed Similarities (Example) ---")
    # for i, product in enumerate(example_products):
    #     print(f"Produit: {product}")
    #     for j, trend in enumerate(example_trends):
    #         # Ensure index j is valid for cosine_similarities_ex columns
    #         if j < cosine_similarities_ex.shape[1]:
    #              print(f"  Tendance: {trend} - Similarité: {cosine_similarities_ex[i][j]:.2f}")
    #         else:
    #              print(f"  Tendance: {trend} - Similarity calculation error or no trend vector")
    # print("-" * 20)

    # --- Using the new function ---
    print("\n--- Ranked Products (using generate_recommendations function) ---")
    ranked_df = generate_recommendations(example_products, example_trends)
    if not ranked_df.empty:
        print(ranked_df)
    else:
        print("Failed to generate recommendations in example.")
