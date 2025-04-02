from pyspark.ml.regression import GBTRegressor

gbt = GBTRegressor(featuresCol="features", labelCol="sales", maxIter=20)
model = gbt.fit(df_sales.join(df_products, "product_id", "left").fillna(0))

model.write().overwrite().save("gs://my-data-bucket/models/trend_model")
