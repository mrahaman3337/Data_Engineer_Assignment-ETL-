from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import regexp_replace, when, col
from pyspark.sql.functions import col

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("Rahaman's_DE_Assignment") \
        .getOrCreate()

    myDF = spark.read \
        .option("header", "true") \
        .option("inferSchema","true") \
        .csv('https://s3.console.aws.amazon.com/s3/buckets/input-bucket-spark/train1.csv?region=us-east-1')


    myDF= myDF.drop("SSN")
    myDF.show(5)

   # data = myDF.select(col("Month").alias('MTHS'))
   # data.show(2)


    columnDF = myDF.withColumn('Annual_Income', regexp_replace('Annual_Income', '_', ''))\
                     .na.drop(subset=['Num_Credit_Inquiries'])\
                     .withColumn('Age', regexp_replace('Age', '_', ''))\
                     .withColumn('Num_of_Loan', regexp_replace('Num_of_Loan', '_', ''))\
                     .withColumn('Amount_invested_monthly', regexp_replace('Amount_invested_monthly', '_', ''))\
                     .withColumn('Num_of_Delayed_Payment', regexp_replace('Num_of_Delayed_Payment', '_', ''))\
                     .withColumn('Occupation', regexp_replace('Occupation', '_______', 'Not specified')) \
                     .withColumn('Payment_Behaviour', regexp_replace('Payment_Behaviour', '!@9#%8', 'Not specified')) \
                     .withColumn('Outstanding_Debt', regexp_replace('Outstanding_Debt', '_', '')) \
                     .withColumn('Credit_Mix', regexp_replace('Credit_Mix', '_', 'Not specified')) \
                     .withColumn('Payment_of_Min_Amount', regexp_replace('Payment_of_Min_Amount', 'NM', 'Not specified')) \
                     .withColumn('Credit_History_Age', myDF.Credit_History_Age.substr(1, 8))
    columnDF.show(20)

    filteredDF = columnDF.filter((columnDF['Age'] >= 0) & (columnDF['Age'] <= 100))\
                      .show(truncate=False)\
                      .filter(columnDF['Num_Credit_Card'] < 10) \
                      .filter((columnDF['Num_of_Loan'] >= 0) & (columnDF['Num_of_Loan'] <= 10)) \
                      .filter(columnDF['Num_Credit_Card'] < 10) \
                      .filter((columnDF['Delay_from_due_date'] >= 0)) \
                      .filter((columnDF['Changed_Credit_Limit'] >= 0)) \
                      .filter((columnDF['Interest_Rate'] < 100)) \
                      .filter((columnDF['Num_Bank_Accounts'] < 15))
    filteredDF.show(50)

    DF = filteredDF.na.fill("Not specified", ["Type_of_Loan"])\
                   .na.fill("Not specified", ["Name"])\
                   .na.fill("Not specified", ["Credit_History_Age"]) \
                   .na.fill("0", ["Num_of_Delayed_Payment"]) \
                   .na.fill("0", ["Amount_invested_monthly"])

    DF.show(30)

    cleaned_DF = DF.withColumn("Monthly_Inhand_Salary", when(DF.Monthly_Inhand_Salary.isNull(), DF.Annual_Income/12)
                      .otherwise(DF.Monthly_Inhand_Salary))\
                      .withColumn('Monthly_Inhand_Salary', functions.round('Monthly_Inhand_Salary', 2))\
                      .withColumn('Credit_History_Age', regexp_replace('Credit_History_Age', 'NA' , 'Not specified'))
    cleaned_DF.show(50)

    myDF.write .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .option("path", "s3://output-bucket-spark/train/") \
        .save()


