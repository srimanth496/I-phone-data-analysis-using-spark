# I-phone-data-analysis-using-spark

first build spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()


Then read the csv file 
spark.read.format("csv").option("header", "true").option("mode", "FAILFAST").option("inferSchema", "true").load("C:/Users/srima/data/apple_data/apple_products.csv")

![image](https://github.com/srimanth496/I-phone-data-analysis-using-spark/assets/84217751/54504727-a150-44e8-b7cd-17e04b273c98)

df.show(20) -- display top 20 rows from the csv file

+--------------------+--------------------+-----+----------+------+-------------------+-----------------+-----------------+----------------+-----------+----+
|        Product Name|         Product URL|Brand|Sale Price|   Mrp|Discount Percentage|Number Of Ratings|Number Of Reviews|             Upc|Star Rating| Ram|
+--------------------+--------------------+-----+----------+------+-------------------+-----------------+-----------------+----------------+-----------+----+
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     49900| 49900|                  0|             3431|              356|MOBEXRGV7EHHTGUH|        4.6|2 GB|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     84900| 84900|                  0|             3431|              356|MOBEXRGVAC6TJT4F|        4.6|2 GB|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     84900| 84900|                  0|             3431|              356|MOBEXRGVGETABXWZ|        4.6|2 GB|
|APPLE iPhone 8 (S...|https://www.flipk...|Apple|     77000| 77000|                  0|            11202|              794|MOBEXRGVMZWUHCBA|        4.5|2 GB|
|APPLE iPhone 8 (G...|https://www.flipk...|Apple|     77000| 77000|                  0|            11202|              794|MOBEXRGVPK7PFEJZ|        4.5|2 GB|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     49900| 49900|                  0|             3431|              356|MOBEXRGVQGYYP8FV|        4.6|2 GB|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     49900| 49900|                  0|             3431|              356|MOBEXRGVQKBREZP8|        4.6|2 GB|
|APPLE iPhone 8 (S...|https://www.flipk...|Apple|     77000| 77000|                  0|            11202|              794|MOBEXRGVZFZGZEWV|        4.5|2 GB|
|APPLE iPhone XS M...|https://www.flipk...|Apple|     89900| 89900|                  0|             1454|              149|MOBF944E2XAHW8V5|        4.6|4 GB|
|Apple iPhone XR (...|https://www.flipk...|Apple|     41999| 52900|                 20|            79512|             6796|MOBF9Z7ZHQC23PWQ|        4.6|4 GB|
|Apple iPhone XR (...|https://www.flipk...|Apple|     39999| 47900|                 16|            79512|             6796|MOBF9Z7ZPHGV4GNH|        4.6|4 GB|
|Apple iPhone XR (...|https://www.flipk...|Apple|     41999| 52900|                 20|            79582|             6804|MOBF9Z7ZS6GF5UAP|        4.6|4 GB|
|Apple iPhone XR (...|https://www.flipk...|Apple|     41999| 52900|                 20|            79512|             6796|MOBF9Z7ZYWNFGZUC|        4.6|3 GB|
|Apple iPhone XR (...|https://www.flipk...|Apple|     41999| 52900|                 20|            79512|             6796|MOBF9Z7ZZY3HCDZZ|        4.6|4 GB|
|APPLE iPhone 11 P...|https://www.flipk...|Apple|    131900|131900|                  0|             1078|              101|MOBFKCTS7HCHSPFH|        4.7|4 GB|
|APPLE iPhone 11 P...|https://www.flipk...|Apple|    117100|117100|                  0|             1078|              101|MOBFKCTSAPAYNSGG|        4.7|4 GB|
|APPLE iPhone 11 P...|https://www.flipk...|Apple|    131900|131900|                  0|             1078|              101|MOBFKCTSCAAKGQV7|        4.7|4 GB|
|APPLE iPhone 11 P...|https://www.flipk...|Apple|    117100|117100|                  0|             1078|              101|MOBFKCTSKDMKCGQS|        4.7|4 GB|
|APPLE iPhone 11 P...|https://www.flipk...|Apple|     74999|106600|                 29|             7088|              523|MOBFKCTSN3TG3RFJ|        4.6|4 GB|
|APPLE iPhone 11 P...|https://www.flipk...|Apple|    117900|140300|                 15|             7088|              523|MOBFKCTSRTHRQTFT|        4.6|4 GB|
+--------------------+--------------------+-----+----------+------+-------------------+-----------------+-----------------+----------------+-----------+----+
only showing top 20 rows

. Display the mMrp of the first 5 Rows using 
  df.select("mrp").show(5)

  .Finding the max mrp by import col and 
  from pyspark.sql.functions import col
from pyspark.sql.functions import *
![image](https://github.com/srimanth496/I-phone-data-analysis-using-spark/assets/84217751/dc07f77f-7981-4c8f-b7fd-38adcf59f63f)

.Display where i phone price mrp  is 149900 
df.where("mrp = 149900").show()
![image](https://github.com/srimanth496/I-phone-data-analysis-using-spark/assets/84217751/f3b6cd77-1bde-465a-97fc-89b2bbcef7e8)

.Display Mrp >50000 of i phone
df.where("Mrp > 50000").show(5)
![image](https://github.com/srimanth496/I-phone-data-analysis-using-spark/assets/84217751/07ccb27f-7047-4688-afdb-806f458a3ab6)

+--------------------+--------------------+-----+----------+-----+-------------------+-----------------+-----------------+----------------+-----------+----+
|        Product Name|         Product URL|Brand|Sale Price|  Mrp|Discount Percentage|Number Of Ratings|Number Of Reviews|             Upc|Star Rating| Ram|
+--------------------+--------------------+-----+----------+-----+-------------------+-----------------+-----------------+----------------+-----------+----+
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     84900|84900|                  0|             3431|              356|MOBEXRGVAC6TJT4F|        4.6|2 GB|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     84900|84900|                  0|             3431|              356|MOBEXRGVGETABXWZ|        4.6|2 GB|
|APPLE iPhone 8 (S...|https://www.flipk...|Apple|     77000|77000|                  0|            11202|              794|MOBEXRGVMZWUHCBA|        4.5|2 GB|
|APPLE iPhone 8 (G...|https://www.flipk...|Apple|     77000|77000|                  0|            11202|              794|MOBEXRGVPK7PFEJZ|        4.5|2 GB|
|APPLE iPhone 8 (S...|https://www.flipk...|Apple|     77000|77000|                  0|            11202|              794|MOBEXRGVZFZGZEWV|        4.5|2 GB|
+--------------------+--------------------+-----+----------+-----+-------------------+-----------------+-----------------+----------------+-----------+----+
only showing top 5 rows

create table from the data frame and perform sql commands on the table
df.createOrReplaceTempView("apple_table")

. perform Group by operations on spark sql
spark.sql("""SELECT 
              `Product Name`,
              SUM(Mrp)
            FROM apple_table
            GROUP BY `Product Name`
        
             """).show()

             +--------------------+--------+
|        Product Name|sum(Mrp)|
+--------------------+--------+
|APPLE iPhone 8 (S...|   77000|
|APPLE iPhone 11 (...|   54900|
|APPLE iPhone 12 P...|  129900|
|APPLE iPhone 11 (...|   59900|
|APPLE iPhone 12 (...|   79900|
|APPLE iPhone 12 (...|   79900|
|Apple iPhone XR (...|   52900|
|APPLE iPhone 12 M...|   74900|
|APPLE iPhone 12 P...|  139900|
|APPLE iPhone 12 P...|  129900|
|APPLE iPhone 12 (...|   84900|
|APPLE iPhone 11 P...|  117100|
|APPLE iPhone SE (...|   44900|
|Apple iPhone SE (...|   54900|
|APPLE iPhone 12 P...|  139900|
|APPLE iPhone XS M...|   89900|
|APPLE iPhone 12 M...|   69900|
|APPLE iPhone 11 P...|  140300|
|APPLE iPhone SE (...|   44900|
|Apple iPhone XR (...|   52900|
+--------------------+--------+
only showing top 20 rows

on top of the group by perform agrregations:
spark.sql("""SELECT 
              `Product Name`,
              SUM(Mrp) sum_mrp
            FROM apple_table
            GROUP BY `Product Name`
        
             """).where("sum_mrp > 100000").show()

![image](https://github.com/srimanth496/I-phone-data-analysis-using-spark/assets/84217751/90b9ad3b-95fc-4c87-b943-6711aac1e1f7)


. created a new column inside the data frame by using with column function 

df.withColumn("dis_price", col("Mrp") * 0.1).show(10)

------+-----------+----+---------+
|        Product Name|         Product URL|Brand|Sale Price|  Mrp|Discount Percentage|Number Of Ratings|Number Of Reviews|             Upc|Star Rating| Ram|dis_price|
+--------------------+--------------------+-----+----------+-----+-------------------+-----------------+-----------------+----------------+-----------+----+---------+
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     49900|49900|                  0|             3431|              356|MOBEXRGV7EHHTGUH|        4.6|2 GB|   4990.0|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     84900|84900|                  0|             3431|              356|MOBEXRGVAC6TJT4F|        4.6|2 GB|   8490.0|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     84900|84900|                  0|             3431|              356|MOBEXRGVGETABXWZ|        4.6|2 GB|   8490.0|
|APPLE iPhone 8 (S...|https://www.flipk...|Apple|     77000|77000|                  0|            11202|              794|MOBEXRGVMZWUHCBA|        4.5|2 GB|   7700.0|
|APPLE iPhone 8 (G...|https://www.flipk...|Apple|     77000|77000|                  0|            11202|              794|MOBEXRGVPK7PFEJZ|        4.5|2 GB|   7700.0|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     49900|49900|                  0|             3431|              356|MOBEXRGVQGYYP8FV|        4.6|2 GB|   4990.0|
|APPLE iPhone 8 Pl...|https://www.flipk...|Apple|     49900|49900|                  0|             3431|              356|MOBEXRGVQKBREZP8|        4.6|2 GB|   4990.0|
|APPLE iPhone 8 (S...|https://www.flipk...|Apple|     77000|77000|                  0|            11202|              794|MOBEXRGVZFZGZEWV|        4.5|2 GB|   7700.0|
|APPLE iPhone XS M...|https://www.flipk...|Apple|     89900|89900|                  0|             1454|              149|MOBF944E2XAHW8V5|        4.6|4 GB|   8990.0|
|Apple iPhone XR (...|https://www.flipk...|Apple|     41999|52900|                 20|            79512|             6796|MOBF9Z7ZHQC23PWQ|        4.6|4 GB|   5290.0|
+--------------------+--------------------+-----+----------+-----+-------------------+-----------------+-----------------+----------------+-----------+----+---------+
only showing top 10 rows







​
