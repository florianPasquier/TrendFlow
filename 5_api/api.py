from fastapi import FastAPI
import pandas as pd

app = FastAPI()
df = pd.read_csv("gs://my-data-bucket/predictions.csv")

@app.get("/recommendations")
def get_recommendations():
    return df.sort_values(by="prediction", ascending=False).head(10).to_dict(orient="records")
