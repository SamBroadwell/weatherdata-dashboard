from pymongo import MongoClient
from urllib.parse import quote_plus
import pprint

USERNAME = "broadws"
PASSWORD = "$unyPoly25!"

USERNAME_ENCODED = quote_plus(USERNAME)
PASSWORD_ENCODED = quote_plus(PASSWORD)

MONGO_URI = (
    f"mongodb+srv://{USERNAME_ENCODED}:{PASSWORD_ENCODED}"
    "@dsa508-test2-mongodb-weather.global.mongocluster.cosmos.azure.com/"
    "?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
)

client = MongoClient(MONGO_URI)
print("✅ Connected to Cosmos DB")

# list all databases
db_names = client.list_database_names()
print("Databases:", db_names)

# use your main DB
db = client["dsa508-test2-mongodb-weather"]

# list all collections inside this DB
collections = db.list_collection_names()
print("Collections:", collections)

# check if 'weatherdata' exists
if "weatherdata" in collections:
    coll = db["weatherdata"]
    count = coll.count_documents({})
    print(f"Document count in 'weatherdata': {count}")
    if count > 0:
        sample = coll.find_one()
        print("Sample document:")
        pprint.pprint(sample)
    else:
        print("⚠️ No documents found in 'weatherdata'.")
else:
    print("⚠️ No collection named 'weatherdata' found.")
