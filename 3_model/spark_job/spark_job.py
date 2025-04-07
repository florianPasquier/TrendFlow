# from pyspark.ml.regression import GBTRegressor

# gbt = GBTRegressor(featuresCol="features", labelCol="sales", maxIter=20)
# model = gbt.fit(df_sales.join(df_products, "product_id", "left").fillna(0))

# model.write().overwrite().save("gs://my-data-bucket/models/trend_model")


from pyspark.sql import SparkSession

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("ProductTrendPrediction") \
    .getOrCreate()

# Charger les données unifiées depuis un CSV ou une base de données (par exemple)
df = spark.read.csv("unified_data.csv", header=True, inferSchema=True)
df.show(5)


from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

# 1. Indexation des variables catégorielles
indexer_category = StringIndexer(inputCol="category", outputCol="category_index")
indexer_name = StringIndexer(inputCol="product_name", outputCol="product_name_index")

# 2. Assembler les features (données d'entrée) en un seul vecteur
assembler = VectorAssembler(
    inputCols=["category_index", "price", "social_trend_score", "historical_sales", "product_name_index"],
    outputCol="features"
)

# Créer un pipeline de transformation
pipeline = Pipeline(stages=[indexer_category, indexer_name, assembler])
model = pipeline.fit(df)
processed_df = model.transform(df)

processed_df.select("features", "target_sales").show(5)


from pyspark.ml.regression import LinearRegression

# Entraîner un modèle de régression linéaire
lr = LinearRegression(featuresCol="features", labelCol="target_sales")

# Entraînement du modèle
lr_model = lr.fit(processed_df)

# Faire des prédictions
predictions = lr_model.transform(processed_df)

# Afficher les résultats
predictions.select("features", "target_sales", "prediction").show(5)


from pyspark.ml.classification import RandomForestClassifier

# Entraîner un modèle de classification (exemple : prédire "tendance" ou "non-tendance")
rf = RandomForestClassifier(featuresCol="features", labelCol="target_sales")

# Entraînement du modèle
rf_model = rf.fit(processed_df)

# Faire des prédictions
predictions = rf_model.transform(processed_df)

# Afficher les résultats
predictions.select("features", "target_sales", "prediction").show(5)


from pyspark.ml.evaluation import RegressionEvaluator

# Évaluateur pour régression
evaluator = RegressionEvaluator(labelCol="target_sales", predictionCol="prediction", metricName="rmse")

# Calcul du RMSE (Root Mean Squared Error)
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) : {rmse}")


from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Évaluateur pour classification
evaluator = MulticlassClassificationEvaluator(labelCol="target_sales", predictionCol="prediction", metricName="accuracy")

# Calcul de la précision
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy : {accuracy}")


# Sauvegarde du modèle et du pipeline
lr_model.save("models/lr_model")
model.save("models/pipeline")

from pyspark.ml import PipelineModel
from pyspark.ml.regression import LinearRegressionModel

# Charger le modèle de régression et le pipeline
loaded_lr_model = LinearRegressionModel.load("models/lr_model")
loaded_pipeline = PipelineModel.load("models/pipeline")
