from dotenv import load_dotenv
from os import getenv, path

load_dotenv()

project_id = getenv("PROJECTID")

print(path.getsize("C:/Users/isabe/revature/repos/TAI-project2/new_data/file.parquet"))