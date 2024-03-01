from pyspark.sql import SparkSession
import json
import os

#Defina SPARK_LOCAL_IP antes de criar a SparkSession
os.environ["SPARK_LOCAL_IP"] = "10.2.1.27"  

master = "spark://10.2.3.249:7077"
spark= SparkSession.builder.master(master).appName("Oracle Spark Connector").config("spark.jars", "/home/maxweber/jupyter-notebook/jars/ojdbc8.jar").getOrCreate() #master
#spark= SparkSession.builder.appName("Oracle Spark Connector").config("spark.jars", "/home/maxweber/jupyter-notebook/jars/ojdbc8.jar").getOrCreate()  #local
sc = spark.sparkContext
#sc.setLogLevel("WARN")


print(spark)


## Carregar credenciais do arquivo JSON
with open("conf.json") as f:
        credencial = json.load(f)["oracle"]

jdbc_url = "jdbc:oracle:thin:@ex8-sf01scan.sefaz.pi.gov.br:1521/SF01"
properties = {
  "user": credencial["user"],
  "password":credencial["password"],
  "driver": "oracle.jdbc.OracleDriver"
   #"driver":"oracle.jdbc.driver.OracleDriver"
}



# Carregar dados do Oracle para DataFrame
try:
    
      
        
    query = "(SELECT 1 as result FROM DUAL) subquery_alias"
          

    
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", query2) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .load()




except Exception as e:
    print("Erro ao conectar ao Oracle e carregar dados:", e)

df.limit(20).show(truncate=False)    

# Encerrando a SparkSession
spark.stop()