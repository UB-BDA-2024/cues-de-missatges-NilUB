from pymongo import MongoClient

class MongoDBClient:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)
        self.database = self.client["MongoDB_"]
        self.collection = self.database["sensors"]

    def close(self):
        self.client.close()
    
    def ping(self):
        return self.client.db_name.command('ping')
    
    def getDatabase(self, database):
        self.database = self.client[database]
        return self.database

    def getCollection(self, collection):
        self.collection = self.database[collection]
        return self.collection
    
    def clearDb(self,database):
        self.client.drop_database(database)

    def insertDoc(self, doc):
        return self.collection.insert_one(doc)
    
    def deleteDoc(self, name):
        self.collection.delete_one({'name': name})

    # Find and delete documents by 'name', because it is indexed in Sensor, and it is also unique
    def findDoc(self, name):
        return self.collection.find_one({'name': name}, {'_id': 0})

    def findNear(self, longitude, latitude, radius):
        """
        Since we have multiple collections, we have to use the find near
        in every collection and then sort by distance, from closest to
        farthest.
        """
        result = []
        db = self.getDatabase()
        collections = db.list_collection_names()
        
        pipeline = [
                {
                    "$geoNear": {
                        "near": {
                            "type": "Point",
                            "coordinates": [longitude, latitude]
                        },
                        "distanceField": "distance",
                        "maxDistance": radius,
                        "spherical": True
                    }
                },
                # '$project' is used to exclude ObjectId from the results
                {
                    "$project": {
                        "_id": 0
                    }
                }
            ]
        
        for collection_name in collections:
            collection = db[collection_name]
            cursor = collection.aggregate(pipeline)
            for doc in cursor:
                result.append(doc)
            
        # Sort by distance
        result = sorted(result, key=lambda doc: doc['distance'])
        return 

