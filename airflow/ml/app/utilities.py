import pickle
import numpy as np


def class_predictor(pickle_file: str, X):
    with open(pickle_file, 'rb') as file:
        class_data = pickle.load(file)
    classifier = class_data["model"]
    scaler = class_data["scaler"]
    X = np.array(X).reshape(1, -1)
    X = scaler.transform(X)
    y_pred = (classifier.predict(X)+1)[0]
    return int(y_pred)


def price_predictor(pickle_file: str, X: list):
    with open(pickle_file, 'rb') as file:
        reg_data = pickle.load(file)

    X = np.array(X, dtype=object).reshape(1, -1)
    regressor_loaded = reg_data["model"]
    le_condition = reg_data["le_condition"]
    le_gas_type = reg_data["le_gas_type"]
    le_transmission = reg_data["le_transmission"]
    le_gear = reg_data["le_gear"]
    le_car_body = reg_data["le_car_body"]
    le_color = reg_data["le_color"]
    le_wheel = reg_data["le_wheel"]
    le_first_name = reg_data["le_first_name"]
    X[:, 2] = le_condition.transform(X[:, 2])
    X[:, 5] = le_gas_type.transform(X[:, 5])
    X[:, 6] = le_transmission.transform(X[:, 6])
    X[:, 7] = le_gear.transform(X[:, 7])
    X[:, 8] = le_car_body.transform(X[:, 8])
    X[:, 9] = le_color.transform(X[:, 9])
    X[:, 10] = le_wheel.transform(X[:, 10])
    X[:, 12] = le_first_name.transform(X[:, 12])
    y_pred = regressor_loaded.predict(X)[0]
    return str(y_pred)
