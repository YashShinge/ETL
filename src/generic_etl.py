import requests
import logging
import pandas as pd
import numpy as np

logging.basicConfig(filename="etl_logs.log",            
                    level=logging.INFO,
                    format="%(levelname)s: %(name)s: %(asctime)s - %(message)s",
                    filemode='w')
logger = logging.getLogger(__name__)


def main():
    """Main ETL script definition.
    
    :returns: None
    """

    # logging
    logger.info("# ETL job is starting")

    # execute ETL pipeline
    data = extract_data()
    data_transformed = transform_data(data)
    load_data(data_transformed)
    
    logger.info('# ETL job completed.')

    return
    
    
def extract_data():
    """Extract data from the API.

    :returns: Pandas DataFrame 
    """

    logger.info("# Extracting data")
    url = "https://feeds.citibikenyc.com/stations/stations.json"
    r = requests.get(url)
    assert r.status_code == 200

    df = pd.DataFrame(r.json()['stationBeanList'])

    return df


def transform_data(df):
    """Transform original data.

    :param df: Input DataFrame.
    :returns: Transformed DataFrame.
    """

    logger.info("# Transforming data")
    cols_to_drop = ['stAddress2', 'city', 'postalCode', 'location', 'altitude', 'landMark']
    df_transformed = df.drop(cols_to_drop, axis=1)

    custom_dtypes = {
                        'stationName': 'string', 
                        'statusKey'  : 'category',
                        'statusValue': 'category',
                        'stAddress1' : 'string',
                        'lastCommunicationTime': 'datetime64[ns]'
                    }
    df_transformed = df_transformed.astype(custom_dtypes)
    
    return df_transformed


def load_data(df):
    """Write data to csv format in local.

    :param df: DataFrame to be stored.
    :returns: None
    """
    
    logger.info("# Loading data")
    df.to_csv('../out/bike_sharing_data.csv', index=False)

    return


# Entry point for ETL.
if __name__ == '__main__':
    main()
    