from logger import *
from MySQL_DB import *
from MongoDB import *

def sql_class():
    lp.info("MySQL_class function invoked")
    try:
        asdf = sql_Database("root", "redhat", "localhost")
        print(asdf.drop_database())
        print(asdf.database_creation())
        print(asdf.table_creation())
        print(asdf.Attribute_bulk_insert())
        print(asdf.DressSales_bulk_insert())
        print(asdf.read_dataset())
        print(asdf.left_join_operation())
        print(asdf.unique_dress_ID_operation())
        print(asdf.total_sell_individual_dress_id())
        print(asdf.recommendation_zero_count_operation())
        print(asdf.third_most_selling_dress())

    except Exception as e:
        return "(sql_class): Failed to execute the functions and variables of mysql_class \n" + str(e)
        lp.error("(sql_class): Failed to execute the functions and variables of mysql_class \n" + str(e))
    finally:
        return "MySQL class operations completed successfully"
        lp.info("MySQL class operations completed successfully")
        lp.info("MySQL class function completed")

def MongoDB_class():
    lp.info("MongoDB class function invoked")
    try:
        lkj = mongo_DB("root", "redhat", "localhost")
        print(lkj.json_convertion())
        print(lkj.drop_collection())
        print(lkj.bulk_insert())

    except Exception as e:
        return "(MongoDB_class): Failed to execute the functions and variables of MongoDBs_class \n" + str(e)
        lp.error("(MongoDB_class): Failed to execute the functions and variables of MongoDBs_class \n" + str(e))
    finally:
        return "MongoDB class operations completed successfully"
        lp.info("MongoDB class operations completed successfully")
        lp.info("MongoDB class function completed")

if __name__ == '__main__':
    try:
        lp.info("Main function invoked and started to execute the database classes")
        sql_class()
        MongoDB_class()
        lp.info("Main function executed successfully \n \n")
    except Exception as e:
        print("(Main function): Failed to execute the database classes \n" + str(e))
        lp.error("(Main function): Failed to execute the database classes \n" + str(e))