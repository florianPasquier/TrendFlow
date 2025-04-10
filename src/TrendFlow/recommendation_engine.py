import pandas as pd
from google.cloud import bigquery
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from dotenv import load_dotenv
import os
import logging
from threading import Lock
from cachetools import cached, TTLCache

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
load_dotenv(".envrc") # Load environment variables from .envrc if present
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_NAME = os.getenv("DATASET_NAME")
TREND_TABLE_NAME = os.getenv("TREND_TABLE_NAME", "trend_data") # Default table names
PRODUCT_TABLE_NAME = os.getenv("PRODUCT_TABLE_NAME", "new_products") # Default table names

TREND_TABLE = f"{PROJECT_ID}.{DATASET_NAME}.{TREND_TABLE_NAME}"
PRODUCT_TABLE = f"{PROJECT_ID}.{DATASET_NAME}.{PRODUCT_TABLE_NAME}"

# --- Cache Configuration ---
# Cache data and model for 1 hour (3600 seconds)
# maxsize=1 ensures only one instance of the engine's data/model is cached
data_cache = TTLCache(maxsize=5, ttl=3600)
model_cache = TTLCache(maxsize=1, ttl=3600)
cache_lock = Lock() # To prevent race conditions during cache initialization

class RecommendationEngine:
    """
    Handles fetching data, building the TF-IDF model, calculating similarity,
    and generating product recommendations based on trends.
    Caches the data and model for efficiency.
    """
    def __init__(self, project_id: str | None = PROJECT_ID, dataset_name: str | None = DATASET_NAME):
        if not project_id or not dataset_name:
            raise ValueError("PROJECT_ID and DATASET_NAME must be set either via environment variables or passed to the constructor.")
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.trend_table = f"{self.project_id}.{self.dataset_name}.{TREND_TABLE_NAME}"
        self.product_table = f"{self.project_id}.{self.dataset_name}.{PRODUCT_TABLE_NAME}"
        self.client = None
        self.trends_df = pd.DataFrame()
        self.products_df = pd.DataFrame()
        self.vectorizer = None
        self.similarity_matrix = None
        self._initialized = False

    def _initialize_client(self):
        """Initializes the BigQuery client."""
        if self.client is None:
            try:
                self.client = bigquery.Client(project=self.project_id)
                logger.info(f"BigQuery client initialized for project {self.project_id}")
            except Exception as e:
                logger.error(f"Failed to initialize BigQuery client: {e}")
                raise ConnectionError(f"Could not connect to BigQuery: {e}")

    @cached(data_cache, lock=cache_lock)
    def _fetch_bq_data(self, query: str) -> pd.DataFrame:
        """Executes a BigQuery query and returns a pandas DataFrame. Cached."""
        self._initialize_client() # Ensure client is initialized
        if self.client is None: return pd.DataFrame() # Return empty if client failed

        try:
            logger.info(f"Executing BQ query (cache key: {query}):\n{query}\n")
            query_job = self.client.query(query)
            results = query_job.result()  # Waits for the job to complete
            df = results.to_dataframe()
            logger.info(f"Fetched {len(df)} rows from BigQuery.")
            return df
        except Exception as e:
            logger.error(f"Error fetching data from BigQuery: {e}")
            return pd.DataFrame() # Return empty DataFrame on error

    def _load_data(self):
        """Loads trends and products data from BigQuery."""
        logger.info("Loading data from BigQuery...")
        # Fetch Trends Data
        trends_query = f"""
            SELECT DISTINCT
                trend_id,
                keyword
            FROM `{self.trend_table}`
            WHERE keyword IS NOT NULL AND TRIM(keyword) != ''
        """
        self.trends_df = self._fetch_bq_data(trends_query)

        # Fetch Products Data
        products_query = f"""
            SELECT DISTINCT
                product_id,
                product_name,
                category
            FROM `{self.product_table}`
            WHERE product_name IS NOT NULL AND TRIM(product_name) != ''
        """
        self.products_df = self._fetch_bq_data(products_query)

        if self.products_df.empty or self.trends_df.empty:
            logger.warning("One or both dataframes (products, trends) are empty after fetching.")
            # Decide if this should be a critical error or just return empty recommendations later
            # For now, log warning and continue, recommendation function will handle empty dfs

        # Combine product name and category
        if not self.products_df.empty:
             # Handle potential missing categories gracefully
            self.products_df['product_text'] = self.products_df['product_name'] + ' ' + self.products_df['category'].fillna('')
            self.products_df = self.products_df[['product_id', 'product_text']].dropna(subset=['product_text'])
        logger.info("Data loading complete.")


    @cached(model_cache, lock=cache_lock)
    def _build_model(self):
        """Builds the TF-IDF vectorizer and calculates the similarity matrix. Cached."""
        if self.products_df.empty or self.trends_df.empty:
            logger.warning("Cannot build model: Products or Trends data is empty.")
            self.vectorizer = None
            self.similarity_matrix = None
            return

        logger.info("Building TF-IDF model and calculating similarity matrix...")
        try:
            # Initialize TF-IDF Vectorizer
            self.vectorizer = TfidfVectorizer(stop_words='english', lowercase=True)

            # Fit and transform the product descriptions
            product_vectors = self.vectorizer.fit_transform(self.products_df['product_text'])

            # Transform the trend keywords
            trend_vectors = self.vectorizer.transform(self.trends_df['keyword'])

            logger.info(f"Product Vectors Shape: {product_vectors.shape}")
            logger.info(f"Trend Vectors Shape: {trend_vectors.shape}")

            # Calculate cosine similarity
            self.similarity_matrix = cosine_similarity(trend_vectors, product_vectors)
            logger.info(f"Similarity Matrix Shape: {self.similarity_matrix.shape}")
            logger.info("Model building complete.")

        except Exception as e:
            logger.error(f"Error building model: {e}")
            self.vectorizer = None
            self.similarity_matrix = None # Ensure model is invalidated on error


    def _ensure_initialized(self):
         """Loads data and builds the model if not already done."""
         # Use a lock to prevent multiple threads/requests initializing concurrently
         with cache_lock:
            if not self._initialized:
                logger.info("Initializing RecommendationEngine...")
                self._load_data() # Fetches data (uses cache)
                self._build_model() # Builds model (uses cache)
                self._initialized = True
                logger.info("RecommendationEngine initialized.")
            else:
                 # Even if initialized, check if cache expired and rebuild if needed
                 # Note: @cached handles TTL, but explicit check might be useful
                 # if _load_data or _build_model could fail silently after init.
                 # For simplicity, relying on @cached TTL for now.
                 pass


    def get_recommendations(self, trend_id: str, top_n: int = 5) -> pd.DataFrame:
        """
        Recommends top N products for a given trend_id.
        Ensures data is loaded and model is built before generating recommendations.
        """
        self._ensure_initialized() # Load data/model if needed

        if self.trends_df.empty or self.products_df.empty or self.similarity_matrix is None:
            logger.warning(f"Cannot generate recommendations for trend {trend_id}: Data or model not available.")
            return pd.DataFrame()

        # Find the index of the trend in our trends_df
        trend_indices = self.trends_df.index[self.trends_df['trend_id'] == trend_id].tolist()

        if not trend_indices:
            logger.warning(f"Trend ID {trend_id} not found in the loaded trends data.")
            # Optional: Could try keyword matching as fallback if needed
            return pd.DataFrame()

        # Use the first index found
        trend_index = trend_indices[0]
        trend_keyword = self.trends_df.loc[trend_index, 'keyword']
        logger.info(f"Finding recommendations for Trend ID: {trend_id} (Keyword: '{trend_keyword}')")

        try:
            # Get the similarity scores for this trend against all products
            # Ensure trend_index is within bounds of the similarity matrix
            if trend_index >= self.similarity_matrix.shape[0]:
                 logger.error(f"Trend index {trend_index} is out of bounds for similarity matrix shape {self.similarity_matrix.shape}")
                 return pd.DataFrame()

            similarity_scores = self.similarity_matrix[trend_index, :]

            # Get the indices of the top N products
            # Ensure we don't request more products than available
            effective_top_n = min(top_n, len(self.products_df))
            # Use argpartition for potentially better performance than full sort for large N
            # For smaller N, argsort is fine.
            # Ensure indices are within bounds of similarity_scores
            if effective_top_n > len(similarity_scores):
                 logger.warning(f"Requested top_n {top_n} exceeds number of similarity scores {len(similarity_scores)}. Adjusting.")
                 effective_top_n = len(similarity_scores)

            # Get indices of top N scores (descending)
            # Adding a check to handle case where effective_top_n might be 0
            if effective_top_n <= 0:
                 top_product_indices = np.array([], dtype=int)
            else:
                 # Use partition to find the k largest elements, then sort only those k
                 partitioned_indices = np.argpartition(similarity_scores, -effective_top_n)[-effective_top_n:]
                 # Get the scores for these top indices and sort them to get the final order
                 top_indices_sorted_by_score = partitioned_indices[np.argsort(similarity_scores[partitioned_indices])[::-1]]
                 top_product_indices = top_indices_sorted_by_score


            # Get the corresponding product IDs and scores
            # Ensure indices are valid for products_df
            valid_indices = [idx for idx in top_product_indices if idx < len(self.products_df)]
            if len(valid_indices) != len(top_product_indices):
                 logger.warning("Some top product indices were out of bounds for products_df.")

            if not valid_indices:
                 logger.info(f"No valid product indices found for trend {trend_id}.")
                 return pd.DataFrame()

            recommended_products = self.products_df.iloc[valid_indices].copy()
            recommended_products['similarity_score'] = similarity_scores[valid_indices]

            # Filter out products with zero or very low similarity (optional threshold)
            similarity_threshold = 0.001
            recommended_products = recommended_products[recommended_products['similarity_score'] >= similarity_threshold]

            logger.info(f"Generated {len(recommended_products)} recommendations for trend {trend_id}.")
            return recommended_products[['product_id', 'product_text', 'similarity_score']]

        except IndexError as e:
             logger.error(f"Index error during recommendation generation for trend {trend_id}: {e}. Matrix/DF shapes might mismatch.")
             return pd.DataFrame()
        except Exception as e:
            logger.error(f"Unexpected error generating recommendations for trend {trend_id}: {e}")
            return pd.DataFrame()

# --- Example Usage (for testing the module directly) ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if not PROJECT_ID or not DATASET_NAME:
        print("Error: Please set PROJECT_ID and DATASET_NAME environment variables or in .envrc")
    else:
        print("Testing RecommendationEngine...")
        try:
            engine = RecommendationEngine()
            # Fetch a trend ID to test with
            engine._ensure_initialized() # Manually initialize for testing
            if not engine.trends_df.empty:
                test_trend_id = engine.trends_df['trend_id'].iloc[0]
                print(f"\nGetting recommendations for trend_id: {test_trend_id}")
                recommendations = engine.get_recommendations(test_trend_id, top_n=10)
                if not recommendations.empty:
                    print("Recommendations found:")
                    print(recommendations)
                else:
                    print("No recommendations generated for this trend (or data/model unavailable).")
            else:
                print("No trends loaded, cannot run recommendation example.")
        except Exception as e:
            print(f"An error occurred during testing: {e}")
