from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()
        self.init_tables()

    def init_tables(self):
            keyspace_query = "CREATE KEYSPACE IF NOT EXISTS sensor WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1};"
            self.session.execute(keyspace_query)

            #Creamos la tabla de temperature
            temperature_table_query = "CREATE TABLE IF NOT EXISTS sensor.temperature(id int, temperature float, PRIMARY KEY(id, temperature));"
            self.session.execute(temperature_table_query)  

            #Creamos la tabla de quantity
            quantity_table_query = "CREATE TABLE IF NOT EXISTS sensor.quantity(id int, type text, PRIMARY KEY(type, id));"
            self.session.execute(quantity_table_query)

            #Creamos la tabla de battery level
            battery_table_query = "CREATE TABLE IF NOT EXISTS sensor.battery(id int, battery_level float, PRIMARY KEY(battery_level, id));"
            self.session.execute(battery_table_query)

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)

    def get_values(self, query, type):

        results = self.session.execute(query)

        sensors = []

        if type == "temperature":
            for row in results:
                sensor = {}
                sensor['id'] = row.sensor_id
                sensor['values'] = [
                    {'max_temperature': row.max_temp,
                     'min_temperature': row.min_temp,
                     'average_temperature': row.avg_temp
                     }]
                sensors.append(sensor)
        elif type == "quantity":
            for row in results:
                sensor = {}
                sensor['type'] = row.type
                sensor['quantity'] = row.quantity
                sensors.append(sensor)
        elif type == "battery":
            for row in results:
                sensor = {}
                sensor['id'] = row.sensor_id
                sensor['battery_level'] = round(row.battery_level, 2)
                sensors.append(sensor)


        result_json = {'sensors': sensors}

        return result_json