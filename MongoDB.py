import pandas as pd
import mysql.connector as connection
import pymongo
import json

from MySQL_DB import sql_Database
from logger import *

class mongo_DB(sql_Database):

    def json_convertion(self):
        lp.info("json convertion function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            df = pd.read_sql('select * from dataset.attribute', mydb)
            df = df.to_json('attribute.json')
            df1 = pd.read_sql('select * from dataset.sales', mydb)
            df1 = df1.to_json('sales.json')
            print("Datasets are converted to json format")
            lp.info("Datasets are converted to json format")

        except Exception as e:
            return "(Json_convertion): Failed to convert the datasets to json \n" + str(e)
            lp.error("(Json_convertion): Failed to convert the datasets to json \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def bulk_insert(self):
        lp.info("Bulk insert function from class invoked")
        try:
            with open('attribute.json', 'r') as f:
                data = json.load(f)
            with open('sales.json', 'r') as f1:
                data1 = json.load(f1)
            lp.info("Json data and data1 loaded successfully")
        except Exception as e:
            print("attribute.json and sales.json files not found \n" + str(e))
            lp.error("attribute.json and sales.json files not found \n" + str(e))
        try:
            client = pymongo.MongoClient("<use your url>")
            db = client.test
            db1 = client['dataset']
            attribute = db1['attribute']
            attribute.insert_many([data])
            sales = db1['sales']
            sales.insert_many([data1])
            result = pd.DataFrame(attribute.find_one())
            print("Attribute data inserted into MongoDB is \n", result)
            lp.info("Attribute data inserted into MongoDB is \n" + str(result))
            result1 =pd.DataFrame(sales.find_one())
            print("sales data inserted into MongoDB is \n", result1)
            lp.info("sales data inserted into MongoDB is \n" + str(result1))
            print("Bulk Data inserted successfully in MongoDB")
            lp.info("Bulk Data inserted successfully in MongoDB")
        except Exception as e:
            return "(bulk_insert): Failed to bulk insert of data to mongoDB \n" + str(e)
            lp.error("(bulk_insert): Failed to bulk insert of data to mongoDB \n" + str(e))
        finally:
            client.close()
            return "MongoDB Database connection closed"
            lp.info("MongoDB Database connection closed")

    def drop_collection(self):
        lp.info("Drop database collection function from class invoked")
        try:
            client = pymongo.MongoClient("mongodb+srv://root:redhat@cluster0.dpqh6.mongodb.net/?retryWrites=true&w=majority")
            db = client.test
            db1 = client['dataset']
            attribute = db1['attribute']
            sales = db1['sales']
            attribute.drop()
            sales.drop()
            print("Database collections dropped successfully")
            lp.info("Database collections dropped successfully")
        except Exception as e:
            return "(drop_collection): Failed to drop the mongoDB collection \n" + str(e)
            lp.error("(drop_collection): Failed to drop the mongoDB collection \n" + str(e))
        finally:
            client.close()
            return "MongoDB Database connection closed"
            lp.info("MongoDB Database connection closed")
