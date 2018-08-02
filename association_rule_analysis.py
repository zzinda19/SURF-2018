import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Cancer_Analysis').getOrCreate()

from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

data_final = spark.read.csv('gs://colleran-weaver-storage/Data-Final/', header=True)
for i in range(1,5):
    level = 'Level_{}'.format(i)
    data_final = data_final.withColumn(level, F.split(F.col(level), '_'))
    data_final = data_final.withColumn(level, F.col(level).getItem(0))
    

df_2 = data_final.filter(data_final.Level_3.isNull() == True)
df_3 = data_final.filter((data_final.Level_3.isNotNull() == True) & (data_final.Level_4.isNull() == True))
df_4 = data_final.filter(data_final.Level_4.isNotNull() == True)

#Level 2
df_2 = df_2.withColumn('L1', df_2.Level_1)
df_2 = df_2.withColumn('L2', F.when(df_2.Level_1 != df_2.Level_2, df_2.Level_2))

df_2 = df_2.withColumn('items', F.array('L1', 'L2'))
df_2 = df_2.withColumn('items', F.sort_array(df_2.items, asc=False))
for i in range(2):
    level = 'L{}'.format(i+1)
    df_2 = df_2.withColumn(level, df_2.items.getItem(i))



df_2_1 = df_2.filter(df_2.L2.isNull() == True)
df_2_2 = df_2.filter(df_2.L2.isNotNull() == True)

df_2_1 = df_2_1.withColumn('items', F.array('L1'))
df_2_1 = df_2_1.select('Patient_ID', 'items')
df_2_2 = df_2_2.withColumn('items', F.array('L1', 'L2'))
df_2_2 = df_2_2.select('Patient_ID', 'items')

data_final_2 = df_2_1.union(df_2_2)
data_final_2 = data_final_2.withColumn('Size', F.size(data_final_2.items))

#Level 3
df_3 = df_3.withColumn('L1', df_3.Level_1)
df_3 = df_3.withColumn('L2', F.when(df_3.Level_1 != df_3.Level_2, df_3.Level_2))
df_3 = df_3.withColumn('L3', F.when((df_3.Level_1 != df_3.Level_3) & (df_3.Level_2 != df_3.Level_3), df_3.Level_3))

df_3 = df_3.withColumn('items', F.array('Level_1', 'L2', 'L3'))
df_3 = df_3.withColumn('items', F.sort_array(df_3.items, asc=False))
for i in range(3):
    level = 'L{}'.format(i+1)
    df_3 = df_3.withColumn(level, df_3.items.getItem(i))



df_3_1 = df_3.filter(df_3.L2.isNull() == True)
df_3_2 = df_3.filter((df_3.L2.isNotNull() == True) & (df_3.L3.isNull() == True))
df_3_3 = df_3.filter(df_3.L3.isNotNull() == True)

df_3_1 = df_3_1.withColumn('items', F.array('L1'))
df_3_1 = df_3_1.select('Patient_ID', 'items')
df_3_2 = df_3_2.withColumn('items', F.array('L2', 'L1'))
df_3_2 = df_3_2.select('Patient_ID', 'items')
df_3_3 = df_3_3.withColumn('items', F.array('L3', 'L2', 'L1'))
df_3_3 = df_3_3.select('Patient_ID', 'items')

data_final_3 = df_3_1.union(df_3_2).union(df_3_3)
data_final_3 = data_final_3.withColumn('Size', F.size(data_final_3.items))

#Level 4
df_4 = df_4.withColumn('L1', df_4.Level_1)
df_4 = df_4.withColumn('L2', F.when(df_4.Level_1 != df_4.Level_2, df_4.Level_2))
df_4 = df_4.withColumn('L3', F.when((df_4.Level_1 != df_4.Level_3) & (df_4.Level_2 != df_4.Level_3), df_4.Level_3))
df_4 = df_4.withColumn('L4', F.when((df_4.Level_1 != df_4.Level_4) & (df_4.Level_2 != df_4.Level_4) & (df_4.Level_3 != df_4.Level_4), df_4.Level_4))

df_4 = df_4.withColumn('items', F.array('L1', 'L2', 'L3', 'L4'))
df_4 = df_4.withColumn('items', F.sort_array(df_4.items, asc=False))
for i in range(4):
    level = 'L{}'.format(i+1)
    df_4 = df_4.withColumn(level, df_4.items.getItem(i))



df_4_1 = df_4.filter(df_4.L2.isNull() == True)
df_4_2 = df_4.filter((df_4.L2.isNotNull() == True) & (df_4.L3.isNull() == True))
df_4_3 = df_4.filter((df_4.L3.isNotNull() == True) & (df_4.L4.isNull() == True))
df_4_4 = df_4.filter(df_4.L4.isNotNull() == True)

df_4_1 = df_4_1.withColumn('items', F.array('L1'))
df_4_1 = df_4_1.select('Patient_ID', 'items')
df_4_2 = df_4_2.withColumn('items', F.array('L2', 'L1'))
df_4_2 = df_4_2.select('Patient_ID', 'items')
df_4_3 = df_4_3.withColumn('items', F.array('L3', 'L2', 'L1'))
df_4_3 = df_4_3.select('Patient_ID', 'items')
df_4_4 = df_4_4.withColumn('items', F.array('L4', 'L3', 'L2', 'L1'))
df_4_4 = df_4_4.select('Patient_ID', 'items')

data_final_4 = df_4_1.union(df_4_2).union(df_4_3).union(df_4_4)
data_final_4 = data_final_4.withColumn('Size', F.size(data_final_4.items))

processed_data = data_final_2.union(data_final_3).union(data_final_4)

from pyspark.ml.fpm import FPGrowth
fp = FPGrowth(minSupport=0.000015, minConfidence=0.001)
fpm = fp.fit(processed_data)

tmp = data_final

l1 = tmp.select('Patient_ID', 'Level_1', 'Age_1', 'Sex')
l2 = tmp.select('Patient_ID', 'Level_2', 'Age_2', 'Sex')
l3 = tmp.select('Patient_ID', 'Level_3', 'Age_3', 'Sex')
l4 = tmp.select('Patient_ID', 'Level_4', 'Age_4', 'Sex')

l3 = l3.withColumnRenamed('Level_3', 'Level')
l3 = l3.withColumnRenamed('Age_3', 'Age')
l3 = l3.dropna(how='any', subset='Level')
l4 = l4.withColumnRenamed('Level_4', 'Level')
l4 = l4.withColumnRenamed('Age_4', 'Age')
l4 = l4.dropna(how='any', subset='Level')
lh = l3.union(l4)

l1.groupBy('Level_1', 'Sex').count().sort('count', ascending=False).show(150, truncate=False)
l2.groupBy('Level_2', 'Sex').count().sort('count', ascending=False).show(150, truncate=False)
lh.groupBy('Level', 'Sex').count().sort('count', ascending=False).show(150, truncate=False)

l1_t = l1.groupBy('Level_1').agg(F.mean('Age_1').alias('Mean'), F.count('Age_1').alias('Count'), F.stddev_samp('Age_1').alias('StdDev'))
l1_t.sort('Count', ascending=False).show(25, truncate=False)
l2_t = l2.groupBy('Level_2').agg(F.mean('Age_2').alias('Mean'), F.count('Age_2').alias('Count'), F.stddev_samp('Age_2').alias('StdDev'))
l2_t.sort('Count', ascending=False).show(25, truncate=False)
lh_t = lh.groupBy('Level').agg(F.mean('Age').alias('Mean'), F.count('Age').alias('Count'), F.stddev_samp('Age').alias('StdDev'))
lh_t.sort('Count', ascending=False).show(25, truncate=False)

aRules = fpm.associationRules
associationRules = fpm.associationRules
freqItemsets = fpm.freqItemsets
freqItemsets = freqItemsets.withColumn('items', F.sort_array('items', asc=True))

for i in range(4):
    colName = 'A{}'.format(i+1)
    associationRules = associationRules.withColumn(colName, F.col('antecedent').getItem(i))


associationRules = associationRules.withColumn('C1', F.col('consequent').getItem(0))

ar_1 = associationRules.filter(F.size(associationRules.antecedent)==1)
ar_2 = associationRules.filter(F.size(associationRules.antecedent)==2)
ar_3 = associationRules.filter(F.size(associationRules.antecedent)==3)

ar_1 = ar_1.withColumn('items', F.array('A1', 'C1'))
ar_2 = ar_2.withColumn('items', F.array('A1', 'A2', 'C1'))
ar_3 = ar_3.withColumn('items', F.array('A1', 'A2', 'A3', 'C1'))

aRules = ar_1.union(ar_2).union(ar_3)
aRules = aRules.withColumn('items', F.sort_array('items', asc=True))
aRules = aRules.join(freqItemsets, aRules.items==freqItemsets.items, 'left')
aRules = aRules.select('antecedent', 'consequent', 'confidence', 'freq')
aRules = aRules.withColumnRenamed('freq', 'frequency')

aRules_1 = aRules.filter(F.size('antecedent')==1)
aRules_h = aRules.filter(F.size('antecedent')!=1)

prRules = aRules_h.filter(F.array_contains('antecedent', 'Prostate')|F.array_contains('consequent', 'Prostate'))
brRules = aRules_h.filter(F.array_contains('antecedent', 'Breast')|F.array_contains('consequent', 'Breast'))
luRules = aRules_h.filter(F.array_contains('antecedent', 'Lung')|F.array_contains('consequent', 'Lung'))
coRules = aRules_h.filter(F.array_contains('antecedent', 'Colon')|F.array_contains('consequent', 'Colon'))
blRules = aRules_h.filter(F.array_contains('antecedent', 'Bladder')|F.array_contains('consequent', 'Bladder'))

pr_rules_1 = aRules_1.filter(F.array_contains('antecedent', 'Prostate'))
pr_rules_2 = aRules_1.filter(F.array_contains('consequent', 'Prostate'))
br_rules_1 = aRules_1.filter(F.array_contains('antecedent', 'Breast'))
br_rules_2 = aRules_1.filter(F.array_contains('consequent', 'Breast'))
lu_rules_1 = aRules_1.filter(F.array_contains('antecedent', 'Lung'))
lu_rules_2 = aRules_1.filter(F.array_contains('consequent', 'Lung'))
co_rules_1 = aRules_1.filter(F.array_contains('antecedent', 'Colon'))
co_rules_2 = aRules_1.filter(F.array_contains('consequent', 'Colon'))
bl_rules_1 = aRules_1.filter(F.array_contains('antecedent', 'Bladder'))
bl_rules_2 = aRules_1.filter(F.array_contains('consequent', 'Bladder'))

pr_rules_1.count()
pr_rules_2.count()
br_rules_1.count()
br_rules_2.count()
lu_rules_1.count()
lu_rules_2.count()
co_rules_1.count()
co_rules_2.count()
bl_rules_1.count()
bl_rules_2.count()

pr_rules_1.toPandas().to_csv('/tmp/pr_rules_1.csv')
pr_rules_2.toPandas().to_csv('/tmp/pr_rules_2.csv')
br_rules_1.toPandas().to_csv('/tmp/br_rules_1.csv')
br_rules_2.toPandas().to_csv('/tmp/br_rules_2.csv')
lu_rules_1.toPandas().to_csv('/tmp/lu_rules_1.csv')
lu_rules_2.toPandas().to_csv('/tmp/lu_rules_2.csv')
co_rules_1.toPandas().to_csv('/tmp/co_rules_1.csv')
co_rules_2.toPandas().to_csv('/tmp/co_rules_2.csv')
bl_rules_1.toPandas().to_csv('/tmp/bl_rules_1.csv')
bl_rules_2.toPandas().to_csv('/tmp/bl_rules_2.csv')