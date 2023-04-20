import glob
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import numpy as np

# Function to extract data from CSV files


def extract_csv(file_to_process):
    df = pd.read_csv(file_to_process, index_col=0)
    return df

# Function to extract data from JSON files


def extract_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df

# Function to extract data from XML files


def extract_xml(file_to_process):
    df = pd.DataFrame(
        columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = str(car.find("fuel").text)
        df = df.append({"car_model": car_model, "year_of_manufacture": year_of_manufacture,
                        "price": price, "fuel": fuel}, ignore_index=True)
    return df

# Function to extract data from all files (CSV, JSON, and XML)


def extract_data():
    data = pd.DataFrame(
        columns=["car_model", "year_of_manufacture", "price", "fuel"])

    # Process all CSV files
    for csvfile in glob.glob("*.csv"):
        data = data.append(extract_csv(csvfile), ignore_index=True)

    # Process all JSON files
    for jsonfile in glob.glob("*.json"):
        data = data.append(extract_json(jsonfile), ignore_index=True)

    # Process all XML files
    for xmlfile in glob.glob("*.xml"):
        data = data.append(extract_xml(xmlfile), ignore_index=True)

    return data

# Function to transform the extracted data (convert price from USD to GBP)


def transform_data(data):
    # Convert price from USD to GBP
    data['price_in_GBP'] = np.round(np.float64(data['price'] * 0.732398), 3)

    # Drop 'price' column (in USD)
    data = data.drop(['price'], axis=1)

    return data

# Function to load transformed data to a CSV file


def load_data(data):
    data.to_csv('transformed_data.csv', index=False)

# Function to log ETL process messages


def log_etl(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # Get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')


# Run ETL process
if __name__ == '__main__':
    log_etl("ETL Job Started")
    log_etl("Extract phase Started")
    data_extracted = extract_data()
    log_etl("Extract phase Ended")
    log_etl("Transform phase Started")
    data_transformed = transform_data(data_extracted)
    log_etl("Transform phase Ended")
    log_etl("Load phase Started")
    load_data(data_transformed)
    log_etl("Load phase Ended")
    log_etl("ETL Job Ended")
