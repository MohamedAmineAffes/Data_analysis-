import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

spark=SparkSession.builder.appName('Analyse').enableHiveSupport().getOrCreate()
df_pyspark=spark.read.option('header','true').csv('big4_financial_risk_compliance.csv')
df_pyspark.printSchema()
df_pyspark.show(3)
df_pyspark=df_pyspark.drop(*['Industry_Affected','Total_Revenue_Impact','AI_Used_for_Auditing','Employee_Workload','Audit_Effectiveness_Score','Client_Satisfaction_Score'])
df_pyspark.show()
df_pyspark=df_pyspark.na.drop(how='any', subset=['Firm_Name']) #Delete any ligne where firmName is null
# 1. Fraud_Cases_Detected 
print(type(df_pyspark['Fraud_Cases_Detected']))
df_pyspark_Fraud_Cases_Detected = df_pyspark.withColumn('Fraud_Cases_Detected', col('Fraud_Cases_Detected').cast(IntegerType()))
df_pyspark_Fraud_Cases_Detected=df_pyspark_Fraud_Cases_Detected.groupBy('Firm_Name').sum('Fraud_Cases_Detected')
df_pyspark_Fraud_Cases_Detected.show()
# Save the DataFrame to Hive (if the table doesn't exist, it will be created)
#df_pyspark_Fraud_Cases_Detected.write.saveAsTable("Fraud_Cases_Detected")
# Verify by running a Hive SQL query
spark.sql("SELECT * FROM Fraud_Cases_Detected").show()
# 2. Compliance_Violations
print(type(df_pyspark['Compliance_Violations']))
df_pyspark_Compliance_Violations = df_pyspark.withColumn('Compliance_Violations', col('Compliance_Violations').cast(IntegerType()))
df_pyspark_Compliance_Violations=df_pyspark_Compliance_Violations.groupBy('Firm_Name').sum('Compliance_Violations')
df_pyspark_Compliance_Violations.show()
# Save the DataFrame to Hive (if the table doesn't exist, it will be created)
#df_pyspark_Compliance_Violations.write.saveAsTable("Compliance_Violations")
# Verify by running a Hive SQL query
spark.sql("SELECT * FROM Compliance_Violations").show()