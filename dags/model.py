import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics
import pickle


# This filepath should be passed as an argument
def train_model(dataset_filepath, trained_model_path, **kwargs):
    dataset = pd.read_csv(dataset_filepath)

    attributes = dataset.iloc[:, 0:4].values
    labels = dataset.iloc[:, 4].values

    # Divide the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(
        attributes, labels, test_size=0.2, random_state=0
    )

    # Feature Scaling
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.transform(X_test)

    # Train the model
    regressor = RandomForestRegressor(n_estimators=200, random_state=0)
    regressor.fit(X_train, y_train)
    y_pred = regressor.predict(X_test)

    # Evaluate the performance of the model
    print('Mean Absolute Error:', metrics.mean_absolute_error(y_test, y_pred))
    print('Mean Squared Error:', metrics.mean_squared_error(y_test, y_pred))
    print('Root Mean Squared Error:', np.sqrt(
        metrics.mean_squared_error(y_test, y_pred)))

    # Save the model to disk

    pickle.dump(regressor, open(trained_model_path, 'wb'))
    print("Model saved.")
