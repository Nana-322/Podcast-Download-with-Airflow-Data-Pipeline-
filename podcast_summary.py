'''Building an airflow data pipeline that downloads podcast metadata from marketplace - A financial news podcast with 50 episodes fixed-
and uses it download the actual podcast episodes.'''
import xmltodict #to parse downloaded episodes 
import requests #to download podcast episodes

from airflow.decorators import dag, task
import pendulum #to define start and end times of downloads

import os

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"


from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

#creating a decorator - dag - to decorate the pipeline function
@dag(
    dag_id='podcast_summary',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 5, 30),
    catchup=False,
)

#writing a function to contain the module/logic of the data pipeline
def podcast_summary():
   
   
    #Task 2 - Creating a sqlite database where the downloaded podcasts will be stored, and configuring a database connection
    create_database = SqliteOperator(
        task_id="create_table_sqlite",
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT
        )
          """,
          sqlite_conn_id="podcasts"
    )


    #Task 1 - Downloads episodes, parses them, and returns them
    @task()
    def get_episodes():
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"] #gives a list of the latest 50 episodes
        print(f"Found {len(episodes)} episodes.")
        return episodes

    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)


    # Task 3 - Storing downloaded episodes into the created database
    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id ="podcasts") #using a hook to make the use of sqlite within python easier
        
        #querying database to find episodes already stored
        stored = hook.get_pandas_df("SELECT * from episodes;")
        new_episodes=[]
        for episode in episodes:
            if episode["link"] not in stored["link"].values:

                #renaming downloaded podcasts
                filename=f"{episode['link'].split('/')[-1]}.mp3"

                #inserting downloaded episodes given specific fields
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])
        hook.insert_rows(table="episodes", rows=new_episodes, target_fields=["link","title","published", "description","filename"])

    load_episodes(podcast_episodes)


    #Task 4 - Downloading episodes
    @task()
    def download_episodes(episodes):
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"

            #creating a full path to stored podcasts
            audio_path = os.path.join("episodes", filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")

                #if podcast is not present, download and write to local hard drive
                audio = requests.get(episode["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)
    
    download_episodes(podcast_episodes)

summary = podcast_summary()