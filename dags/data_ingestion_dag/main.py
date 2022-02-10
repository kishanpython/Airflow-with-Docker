# We'll start by importing the DAG object
from datetime import timedelta, datetime
from pathlib import Path

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

# other libs
import pandas as pd
import sqlite3
import os
import requests
from bs4 import BeautifulSoup
import shutil
# get dag directory path
dag_path = os.getcwd()

# data scraping from inshort
def scrape_data(exec_date):
	try:
		print(f"Ingesting data for date: {exec_date}")
		date = datetime.strptime(exec_date, '%Y-%m-%d %H')
		file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
		print(f"File date path:- {file_date_path}")

		# News Types
		seed_urls = ['https://inshorts.com/en/read/technology',
		             'https://inshorts.com/en/read/sports',
		             'https://inshorts.com/en/read/world',
		             'https://inshorts.com/en/read/politics',
		             'https://inshorts.com/en/read/entertainment',
		             'https://inshorts.com/en/read/automobile',
		             'https://inshorts.com/en/read/science',
		             'https://inshorts.com/en/read/world']

		news_data = []

		# Collecting data
		for url in seed_urls:
		    news_category = url.split('/')[-1]
		    data = requests.get(url)
		    soup = BeautifulSoup(data.content, 'html.parser')
		    news_articles = [{'news_headline': headline.find('span', 
		                                                        attrs={"itemprop": "headline"}).string,
		                        'news_article': article.find('div', 
		                                                    attrs={"itemprop": "articleBody"}).string,
		                        'news_category': news_category}
		                        
		                        for headline, article in 
		                            zip(soup.find_all('div', 
		                                            class_=["news-card-title news-right-box"]),
		                                soup.find_all('div', 
		                                            class_=["news-card-content news-right-box"]))
		                    ]
		    news_data.extend(news_articles)

		# Creating dataframe     
		df =  pd.DataFrame(news_data)
		df = df[['news_headline', 'news_article', 'news_category']] 
		print(df.head())

		# load processed data
		output_dir = Path(f'{dag_path}/scraped_data/{file_date_path}')
		output_dir.mkdir(parents=True, exist_ok=True)
		# scraped_data/2021-08-15/12/2021-08-15_12.csv
		df.to_csv(output_dir / f"{file_date_path}.csv".replace("/", "_"), index=False)

	except:
		print("No data scraped")

# Data pre-processing 
def data_processing(exec_date):
	# path to read the file
	file_loc_path = f"{dag_path}/scraped_data"
	print(f"pre-preocssing  data for date: {exec_date}")

	# date and time extracting from the datetime module
	date = datetime.strptime(exec_date, '%Y-%m-%d %H')
	file_date_path = f"{date.strftime('%Y-%m-%d')}/{date.hour}"
	name_of_the_file = file_date_path.replace("/","_")

	try:
		# actively scraped data path
		current_scrap_data_path = f"{dag_path}/scraped_data/{file_date_path}/{file_date_path.replace('/', '_')}.csv"

		# current data path
		current_df_data = pd.read_csv(current_scrap_data_path,index_col=None)

		# append data path
		merger_data_path = f"{dag_path}/final_data/final_data.csv"

		# append the scrape data 
		current_df_data.to_csv(merger_data_path, mode='a', index=False, header=False)

		# To archived the data we scraped
		# archived_data_path = f"{dag_path}/archived_data/"
		# archived_data_file_name = f"{archived_data_path}/{name_of_the_file}_data"
		# shutil.make_archive(archived_data_file_name, 'zip', archived_data_path)
	except:
		print("Their is some issue related to final data saving")



# Initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 2, 9)
    
}


# Creation of DAG object 
ingestion_dag = DAG(
    'scraping_ingestion',
    default_args=default_args,
    description='Scrape data from inshorts',
    schedule_interval = '@hourly',
    catchup=False,
)

# Scrape the data from Inshort-News-Web-App
scrape_data = PythonOperator(
    task_id='srape_data',
    python_callable=scrape_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=ingestion_dag,
)

# After Scraping the latest news we save the data 
# in a scraped_data folder and within it with date and 
# hourly time basis
# Ex:- scraped_data/2022-02-10/6/2022-02-10_6.csv
 
data_processing = PythonOperator(
    task_id='data_processing',
    python_callable=data_processing,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=ingestion_dag,
)

# Task priority 
scrape_data >> data_processing