from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime


from shared.cassandra_client import CassandraClient
from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale
from shared.elasticsearch_client import ElasticsearchClient

import json
from . import models, schemas


class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def get_sensor_mongo(mongodb: MongoDBClient, db_sensor: models.Sensor) -> schemas.SensorCreate:
    sensor_dict = mongodb.getCollection('sensors').find_one({'name': db_sensor.name}, {'_id': 0})
    sensor_dict.update({'id': db_sensor.id})
    return sensor_dict

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient, esdb: ElasticsearchClient, cassandra: CassandraClient) -> schemas.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude, type=sensor.type, mac_address=sensor.mac_address, manufacturer=sensor.manufacturer, model=sensor.model, serie_number=sensor.serie_number, firmware_version=sensor.firmware_version, description=sensor.description)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    mongodb.insertDoc(sensor.dict())
    
    #Afegim el name, type i description
    data = {
        'name': sensor.name,
        'type': sensor.type,
        'description': sensor.description
    }
    esdb.index_document(index_name="sensors", document=data)
    sensor_dict = sensor.dict()
    sensor_dict.update({'id': db_sensor.id})

    #Guardamos el id y el type en la tabla quantity
    query_increment = f"INSERT INTO sensor.quantity(id, type) VALUES ({db_sensor.id}, '{sensor.type}');"
    cassandra.execute(query_increment)
    return sensor_dict

def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData, timescale: Timescale, cassandra: CassandraClient) -> schemas.SensorData:
    #Cogemos los datos de data y lo pasamos a un diccionario
    data_sensor = data.dict()

    #Tenemos que transformar los datos que sean None a Null para poderlas poner en SQL
    new_data = {key: ('NULL' if value is None else value if key != 'last_seen' else f"'{value}'") for key, value in data_sensor.items()}

    #Creamos la query para añadir los datos a TimescaleDB
    query_sin_parametros = "INSERT INTO sensor_data (id, temperature, humidity, velocity, battery_level, last_seen) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id, last_seen) DO UPDATE SET temperature = EXCLUDED.temperature, humidity = EXCLUDED.humidity, velocity = EXCLUDED.velocity, battery_level = EXCLUDED.battery_level;"
    query = query_sin_parametros % (sensor_id, new_data['temperature'], new_data['humidity'], new_data['velocity'], new_data['battery_level'], new_data['last_seen'])
    timescale.execute(query)
    timescale.execute("commit")

    #Guardamos la temperatura
    if data.temperature is not None:
        query = f"INSERT INTO sensor.temperature(id, temperature) VALUES ({sensor_id}, {data.temperature});"
        cassandra.execute(query)

    #Guardamos la battery
    cassandra.execute(f"INSERT INTO sensor.battery(id, battery_level) VALUES ({sensor_id}, {data.battery_level});")

    redis.set(sensor_id, json.dumps(data_sensor))
    return json.loads(redis.get(sensor_id))

def get_data(redis: RedisClient, db_sensor: models.Sensor):
    db_data = redis.get(db_sensor.id)

    if db_data is None:
        raise HTTPException(status_code=404, detail="Sensor data not found")
    json_data = json.loads(db_data)
    # Create the dictionary for return with id or name if exists
    db_data = {
        'id': db_sensor.id,
        'name': db_sensor.name
    }
    # We add the information of get_Data at id and name
    db_data.update(json_data)
    return db_data

def get_ts_data(ts: Timescale, sensor_id: int, from_date: str, end_date: str, bucket: int) -> schemas.SensorData:
    """
    Retrieve data from timescale grouped by bucket (it can be hour, day and week).
    """
    ts_query = f"""
        SELECT *
        FROM {bucket}
        WHERE sensor_id = {sensor_id}
            AND {bucket} BETWEEN time_bucket('1 {bucket}', '{from_date}'::timestamp)
            AND time_bucket('1 {bucket}', '{end_date}'::timestamp)
        ORDER BY {bucket} ASC;
    """

    ts.execute(ts_query)
    result = ts.getCursor().fetchall()

    return result

def delete_sensor(db: Session, mongodb: MongoDBClient, redis: RedisClient, esdb: ElasticsearchClient, ts: Timescale, sensor_id: int):
    """
    Delete all the data from sensor_id from mongo, model, redis , elasticsearch and timescale
    """
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    mongodb.deleteDoc(db_sensor.name)
    redis.delete(sensor_id)
    #Query per fer el delete de la id
    ts_delete_query = f"""DELETE FROM {'sensor_data'} WHERE sensor_id={sensor_id}"""
    ts.execute(ts_delete_query)
    return db_sensor

def search_sensors(db: Session, mongodb: MongoDBClient, elasticdb: ElasticsearchClient, query:str, size:int, search_type:str):
    search_sensors_dict = []
    final_query = get_query(query, search_type)
    query_results = elasticdb.search('sensors', final_query, size)

    for doc in query_results['hits']['hits']:
        db_sensor = get_sensor_by_name(db, doc['_source']['name'])
        mongo_sensor = get_sensor_mongo(mongodb, db_sensor)
        search_sensors_dict.append(mongo_sensor)

    return search_sensors_dict

# Auxiliar function
def get_query(query:str, search_type:str):
    final_query = {}
    query = json.loads(query.lower())
    if search_type in ['match', 'prefix']:
        final_query = {
            'query': {
                search_type: query
            }
        }
    elif search_type == 'similar':
        key = list(query)[0]
        final_query = {
            "query": {
                "match": {
                    key: {
                        "query": query[key],
                        "fuzziness": 'auto',
                        'operator': 'and'
                    }
                }
            }
        }

    return final_query



def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float, radius: float, redis: RedisClient, db: Session):
    list_near = []
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
     "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}

    sensors = mongodb.collection.find(query)
    for sensor in sensors:
        db_sensor = get_sensor_by_name(db, sensor['name'])
        db_sensor_data = get_data(redis, db_sensor)
        list_near.append(db_sensor_data)

    return list_near

def getView(bucket: str) -> str:
    if bucket == 'year':
        return 'sensor_data_yearly'
    if bucket == 'month':
        return 'sensor_data_monthly'
    if bucket == 'week':
        return 'sensor_data_weekly'
    if bucket == 'day':
        return 'sensor_data_daily'
    elif bucket == 'hour':
        return 'sensor_data_hourly'
    else:
        raise ValueError("Invalid bucket size")
    
def get_cassandra_values(db:Session, cassandra:CassandraClient, type:str, mongodb:MongoDBClient):
    query_temperature = """SELECT sensor_id, AVG(temperature) AS avg_temp, MIN(temperature) AS min_temp, MAX(temperature) AS max_temp
                FROM sensor.temperature 
                GROUP BY sensor_id;"""

    query_quantity = """
            SELECT * FROM sensor.quantity;
            """

    query_battery = """
            SELECT sensor_id, battery_level
            FROM sensor.battery
            WHERE battery_level < 0.2
            ALLOW FILTERING;
            """

    result_json = {}

    if type == "temperature":
        result_json = cassandra.get_values(query_temperature, type)
    elif type == "quantity":
        result_json = cassandra.get_values(query_quantity, type)
    elif type == "battery":
        result_json = cassandra.get_values(query_battery, type)

    if type != "quantity":
        for sensor in result_json['sensors']:
            db_sensor = get_sensor(db, sensor['id'])
            mongo_sensor = get_sensor_mongo(mongodb, db_sensor)
            sensor.update(mongo_sensor)


    return result_json