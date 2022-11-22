import os
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

from app.utilities import class_predictor

classifier_pickle_file = os.path.abspath("02_classification.pkl")
# regressor_pickle_file = os.path.abspath("app/01_regression.pkl")

cwd = os.getcwd()  # Get the current working directory (cwd)
files = os.listdir(cwd)  # Get all the files in that directory
print("Files in %r: %s" % (cwd, files))

app = FastAPI()


class Item(BaseModel):
    ad_id: int
    price: Optional[int]
    year: Optional[int]
    mileage: Optional[int]
    engine_power: Optional[int]
    engine_vol: Optional[float]
    n_owners: Optional[int]


@app.get('/')
async def root():
    return {"values": "Welcome, i\'m running from docker fastapi"}


@app.post('/predict_class')
async def predict_class(items: list[Item]):
    result = []
    print(Item)
    for i in items:
        ad_id = list(i.dict().values())[0]
        features = list(i.dict().values())[1:]
        try:
            prediction = class_predictor(classifier_pickle_file, features)
        except:
            prediction = None
        prediction = {'ad_id': ad_id, "predicted_class": prediction}
        result.append(prediction)
    return result


# @app.route("/predict_price", methods=["POST"])
# def predictor_price():
#     # data = json.loads(request.data)
#     data = data["values"]
#     # predictions = price_predictor(regressor_pickle_file, data)
#     predictions = data
#     return predictions

