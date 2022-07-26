import pandas as pd
import mysql.connector as connection
from logger import *


class sql_Database:
    def __init__(self, username, password, host):
        lp.info("Sql_database class invoked")
        try:
            self.host = host
            self.username = username
            self.password = password
            lp.info("The MySQL DB connection parameters initialized")
        except Exception as e:
            return "__init__: Failed to initialize \n" + str(e)
            lp.error("__init__: Failed to initialize \n" + str(e))

    def database_creation(self):
        lp.info("Database creation function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            query = "Create database if not exists dataset"
            query1 = "show databases"
            cursor = mydb.cursor()
            cursor.execute(query)
            cursor.execute(query1)
            output = cursor.fetchall()
            print("Database created successfully  ", output)
            lp.info("Database created successfully  " + str(output))

        except Exception as e:
            return "(database_creation): Failed to create sql database \n" + str(e)
            lp.error("(database_creation): Failed to create sql database \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def table_creation(self):
        lp.info("Table creation function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            query = "create table if not exists dataset.attribute(Dress_ID int ," \
                    "Style varchar(100) , " \
                    "Price varchar(100) , " \
                    "Rating float(20,10) , " \
                    "Size varchar(100) , " \
                    "Season varchar(100) ," \
                    "NeckLine varchar(100) ," \
                    "SleeveLength varchar(100) ," \
                    "waiseline varchar(100) ," \
                    "Material varchar(100) ," \
                    "FabricType varchar(100) ," \
                    "Decoration varchar(100) ," \
                    "PatternType varchar(100) ," \
                    "Recommendation int )"
            query1 = "create table if not exists dataset.sales(Dress_ID int ,`29/8/2013` int , `31/8/2013` int , `09-02-2013` int," \
                     "`09-04-2013` int,`09-06-2013` int,`09-08-2013` int,`09-10-2013` int,`09-12-2013` int,`14/9/2013` int," \
                     "`16/9/2013` int, `18/9/2013` int, `20/9/2013` int, `22/9/2013` int, `24/9/2013` int, `26/9/2013` int, " \
                     "`28/9/2013` int,`30/9/2013` int,`10-02-2013` int,`10-04-2013` int,`10-06-2013` int,`10-08-2010` int," \
                     "`10-10-2013` int,`10-12-2013` int)"
            query2 =  "show tables from dataset"
            cursor.execute(query)
            cursor.execute(query1)
            cursor.execute(query2)
            output = cursor.fetchall()
            print("Tables created successfully", output)
            lp.info("Tables created successfully"+ str(output))

        except Exception as e:
            return "(table_creation): Failed to create table \n"+ str(e)
            lp.info("(table_creation): Failed to create table \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def Attribute_bulk_insert(self):
        lp.info("Attribute bulk insert function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            df = pd.read_excel(r"C:\Users\Babu Jayaraman\Music\FSDS_Live_classes_Tasks\Database_Task\Attribute DataSet.xlsx")
            df.fillna('NULL', inplace=True)
            try:
                for (row, rs) in df.iterrows():
                    Dress_ID = str(int(rs[0]))
                    Style = rs[1]
                    Price = rs[2]
                    Rating = str(float(rs[3]))
                    Size = rs[4]
                    Season = rs[5]
                    NeckLine = rs[6]
                    SleeveLength = rs[7]
                    waiseline = rs[8]
                    Material = rs[9]
                    FabricType = rs[10]
                    Decoration = rs[11]
                    PatternType = rs[12]
                    Recommendation = str(int(rs[13]))
                    asdf = (Dress_ID + "," + "'{}'" + "," + "'{}'" + "," + Rating + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + "'{}'" + "," + Recommendation).format(
                        Style, Price, Size, Season, NeckLine, SleeveLength, waiseline, Material, FabricType, Decoration,PatternType)
                    query = "insert into dataset.attribute values({})".format(asdf)
                    cursor.execute(query)
                mydb.commit()
                print("All attribute data inserted")
                lp.info("All attribute data inserted")
            except Exception as e:
                print("Error in inserting attribute bulk data \n" + str(e))
                lp.error("Error in inserting attribute bulk data \n" + str(e))
        except Exception as e:
            return "(Attribute_bulk_upload): Failed to bulk upload Attribute data to attribute table \n"+ str(e)
            lp.error("(Attribute_bulk_upload): Failed to bulk upload Attribute data to attribute table \n"+ str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def DressSales_bulk_insert(self):
        lp.info("Dress Sales bulk insert function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            df = pd.read_excel(r"C:\Users\Babu Jayaraman\Music\FSDS_Live_classes_Tasks\Database_Task\Dress Sales.xlsx")

            ## NULL, Removed, removed and Orders values are handled(replaced) below as 0
            df.fillna(0, inplace=True)
            df.replace(to_replace='removed', value=0, inplace=True)
            df.replace(to_replace='Removed', value=0, inplace=True)
            df.replace(to_replace='Orders', value=0, inplace=True)

            try:
                for (row, rs) in df.iterrows():
                    Dress_ID = str(int(rs[0]))
                    a = str(int(rs[1]))
                    b = str(int(rs[2]))
                    c = str(int(rs[3]))
                    d = str(int(rs[4]))
                    e = str(int(rs[5]))
                    f = str(int(rs[6]))
                    g = str(int(rs[7]))
                    h = str(int(rs[8]))
                    i = str(int(rs[9]))
                    j = str(int(rs[10]))
                    k = str(int(rs[11]))
                    l = str(int(rs[12]))
                    m = str(int(rs[13]))
                    n = str(int(rs[14]))
                    o = str(int(rs[15]))
                    p = str(int(rs[16]))
                    q = str(int(rs[17]))
                    r = str(int(rs[18]))
                    s = str(int(rs[19]))
                    t = str(int(rs[20]))
                    u = str(int(rs[21]))
                    v = str(int(rs[22]))
                    w = str(int(rs[23]))

                    asdf = (Dress_ID + "," + a + "," + b + "," + c + "," + d + "," + e + "," + f + "," + g + "," + h + "," + i + "," + j + "," + k + "," + l + "," + m + "," + n + "," + o + "," + p + "," + q + "," + r + "," + s + "," + t + "," + u + "," + v + "," + w)
                    query = "insert into dataset.sales values({})".format(asdf)
                    cursor.execute(query)
                mydb.commit()
                print("All sales data inserted")
                lp.info("ALl sales data inserted")
            except Exception as e:
                return "Error in inserting sales bulk data \n" + str(e)
                print("Error in inserting sales bulk data \n" + str(e))
        except Exception as e:
            return "(DressSales_bulk_upload): Failed to bulk upload DressSales data to the sales table \n"+ str(e)
            lp.error("(DressSales_bulk_upload): Failed to bulk upload DressSales data to the sales table \n"+ str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def read_dataset(self):
        lp.info("Read dataset function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            result = pd.read_sql('select * from dataset.attribute', mydb)
            print(result)
            lp.info("Attribute dataset is: \n" + str(result))

            result1 = pd.read_sql('select * from dataset.sales', mydb)
            print(result1)
            lp.info("sales dataset is: \n" + str(result1))
            print("Attribute and sales Dataset read completed successfully")
            lp.info("Attribute and sales Dataset read completed successfully")

        except Exception as e:
            return "(read_dataset): Failed to read the attribute and sales datasets in pandas dataframe \n"+ str(e)
            lp.error("(read_dataset): Failed to read the attribute and sales datasets in pandas dataframe \n"+ str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def left_join_operation(self):
        lp.info("left join operation function from class is invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            cursor.execute("use dataset")
            query = "select attribute.Dress_ID, attribute.Style, attribute.Price, Rating, sales.Dress_ID,sales.`29/8/2013`, sales.`31/8/2013` from attribute left join sales on attribute.Dress_ID = sales.Dress_ID"
            cursor.execute(query)
            result = cursor.fetchall()
            print(result)
            lp.info("Left join operation output is : \n" + str(result))
            print("Left Join operation completed successfully")
            lp.info("Left Join operation completed successfully")

        except Exception as e:
            return "(left_join_operation): Failed Left join operation \n"+ str(e)
            lp.error("(left_join_operation): Failed Left join operation \n"+ str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def unique_dress_ID_operation(self):
        lp.info("unique dress Id operation function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            query = "select distinct Dress_ID from dataset.attribute"
            cursor.execute(query)
            result = cursor.fetchall()
            print(result)
            lp.info("The unique dress id from attribute dataset is : \n" + str(result))

            query1 = "select  count(DISTINCT Dress_ID) from dataset.attribute"
            cursor.execute(query1)
            result1 = cursor.fetchall()
            print("Count of Unique Dresses found is ", result1)
            lp.info("Count of Unique Dresses found is " + str(result1))
            lp.info("Function completed")

        except Exception as e:
            return "(unique_dress_ID_operation): Failed to find the unique dresses \n" + str(e)
            lp.error("(unique_dress_ID_operation): Failed to find the unique dresses \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def recommendation_zero_count_operation(self):
        lp.info("recommendation zero count operation function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            query = "select count(*) from dataset.attribute where Recommendation = 0"
            cursor.execute(query)
            result = cursor.fetchall()
            print("Count of attribute dataset where recommendation is 0 is ", result)
            lp.info("Count of attribute dataset where recommendation is 0 is " + str(result))

        except Exception as e:
            return "(recommendation_zero_count_operation): Failed to count the recommendation value 0 \n" + str(e)
            lp.error("(recommendation_zero_count_operation): Failed to count the recommendation value 0 \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def total_sell_individual_dress_id(self):
        lp.info("Total sell individual dress id function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            query = "select Dress_ID , count(*) from dataset.attribute group by Dress_ID"
            cursor.execute(query)
            result = cursor.fetchall()
            print(result)
            lp.info("Total count of dress for individual dress id is: \n" + str(result))
            print("Total dress sold for individual dress id is found")
            lp.info("Total dress sold for individual dress id is found")

        except Exception as e:
            return "(total_sell_individual_dress_id): Failed to find the total sell for the individual dress id \n" + str(e)
            lp.error("(total_sell_individual_dress_id): Failed to find the total sell for the individual dress id \n" + str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def third_most_selling_dress(self):
        lp.info("Third most selling dress function from class invoked")
        try:
            mydb = connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            query = "select Dress_ID , count(*) from dataset.sales group by Dress_ID order by count(*) DESC LIMIT 3, 1"
            cursor.execute(query)
            result = cursor.fetchall()
            print("The third most selling dress is: ", result)
            lp.info("The third most selling dress is: " + str(result))

        except Exception as e:
            return "(third_most_selling_dress): Failed to find the third most selling dress \n" + str(e)
            lp.error("(third_most_selling_dress): Failed to find the third most selling dress \n" + str(e))

        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")

    def drop_database(self):
        lp.info("Drop database function from class invoked")
        try:
            mydb =  connection.connect(host=self.host, user=self.username, passwd=self.password)
            cursor = mydb.cursor()
            cursor.execute("drop database dataset")
            print("Database dropped successfully")
            lp.info("Database dropped successfully")

        except Exception as e:
            return "(drop_database): Failed dropping database \n"+ str(e)
            lp.info("(drop_database): Failed dropping database \n"+ str(e))
        finally:
            mydb.close()
            return "MySQL Database connection closed"
            lp.info("MySQL Database connection closed")


