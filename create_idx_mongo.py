from pymongo import MongoClient

# Replace with your actual MongoDB URI
mongo_uri = "mongodb://dev-valuemind:W57mFPVT57lt3wU@10.10.0.42:27021/?replicaSet=rs0&directConnection=true&authSource=assets-valuemind"

client = MongoClient(mongo_uri)

# Use the correct database and collection
db = client["assets-valuemind"]
collection = db["test-dim"]

collection.create_index([("assetsCompareManagements.assetsManagement.geoJsonPoint", "2dsphere")])
