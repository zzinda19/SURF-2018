import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Cancer_Analysis').getOrCreate()

# Defining the names of the text files and records to obtain.
files = ['BREAST', 'COLRECT', 'DIGOTHR', 'FEMGEN', 'LYMYLEUK', 'MALEGEN', 'OTHER', 'RESPIR', 'URINARY']
cols = ['Patient_ID', 'Race', 'Sex', 'Age_DX', 'MN_DX', 'YR_DX', 'Seq_Num', 'Primary_Site', 'Malignant']
starts = [1, 234, 24, 25, 37, 39, 35, 230, 224]
lengths = [8, 1, 1, 3, 2, 4, 2, 3, 1]
old_path = 'gs://summer-research-bucket-18/medical-data/Seer/Seer'
path = 'gs://colleran-weaver-storage/SEER/SEER_'
primary_schema = spark.read.csv('gs://colleran-weaver-storage/schema.csv', inferSchema=True, header=True)

# Organizing main imports.
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql import functions as F

# Initializing the dataframes so that each text file can be unioned dynamically.
d = Row(value='temp')
df_1 = spark.createDataFrame(d, StringType())
df_2 = spark.createDataFrame(d, StringType())
df_3 = spark.createDataFrame(d, StringType())
df_4 = spark.createDataFrame(d, StringType())

# Unioning the text files from each folder.
for i in range(9):
   df_temp_1 = spark.read.text('{}1/{}.TXT'.format(path, files[i]))
   df_temp_2 = spark.read.text('{}2/{}.TXT'.format(path, files[i]))
   df_temp_3 = spark.read.text('{}3/{}.TXT'.format(path, files[i]))
   df_temp_4 = spark.read.text('{}4/{}.TXT'.format(path, files[i]))
   df_1 = df_1.union(df_temp_1)
   df_2 = df_2.union(df_temp_2)
   df_3 = df_3.union(df_temp_3)
   df_4 = df_4.union(df_temp_4)


# Unioning the complete text files from each folder into one dataframe and separating the desired columns.
df = df_1.union(df_2).union(df_3).union(df_4)
for i in range(9):
   df = df.withColumn(cols[i], df.value.substr(starts[i], lengths[i]))


df = df.drop('value')
df = df.filter(F.length('Patient_ID') == 8)

# SEER Dictionary specifies that Categories 1,2,5 are non-malignant tumors, while Categories 3,4,6 are malignant tumors.
df = df.filter(df.Malignant.isin(3,4,6) == True)
df = df.withColumn('Seq_Num', df.Seq_Num.cast(IntegerType()))
df = df.withColumn('Age_DX', df.Age_DX.cast(IntegerType()))
df = df.withColumn('MN_DX', df.MN_DX.cast(IntegerType()))
df = df.withColumn('YR_DX', df.YR_DX.cast(IntegerType()))

df = df.withColumn('Primary_Site', df.Primary_Site.cast(IntegerType()))
df = df.join(primary_schema, df.Primary_Site==primary_schema.Code)
df = df.drop('Code')
df = df.withColumn('Primary_Site', df.Description)
df = df.drop('Description')

df = df.withColumn('Date_DX', (df.MN_DX/12) + df.YR_DX)
df = df.sort('Seq_Num')

df_01 = df_21.groupBy('Patient_ID').agg(F.collect_list('Primary_Site').alias('items'), F.collect_list('Age_DX').alias('Ages'), F.collect_list('Seq_Num').alias('Sequence'),
                                     F.collect_list('Sex').alias('Sex'), F.collect_list('MN_DX').alias('Months'), F.collect_list('YR_DX').alias('Years'))

df_01 = df.groupBy('Patient_ID').agg(F.collect_list('Primary_Site').alias('items'), F.collect_list('Age_DX').alias('Ages'), F.collect_list('Seq_Num').alias('Sequence'),
                                     F.collect_list('Sex').alias('Sex'), F.collect_list('MN_DX').alias('Months'), F.collect_list('YR_DX').alias('Years'))

df_01 = df.groupBy('Patient_ID').agg(F.collect_list('Primary_Site').alias('items'), F.collect_list('Seq_Num').alias('Sequence'), F.collect_list('Date_DX').alias('Dates'))

df_01 = df_01.withColumn('Size', F.size('items'))
df_01 = df_01.filter((df_01.Size < 5) & (df_01.Size != 1))
df_02 = df_01.filter(df_01.Size == 2)
df_03 = df_01.filter(df_01.Size == 3)
df_04 = df_01.filter(df_01.Size == 4)

for i in range(4):
    j = i + 1
    seq = 'Seq_{}'.format(j)
    date = 'Date_{}'.format(j)
    df_04 = df_04.withColumn(seq, F.col('Sequence').getItem(i))
    df_04 = df_04.withColumn(date, F.col('Dates').getItem(i))
    if (i < 3):
        df_03 = df_03.withColumn(seq, F.col('Sequence').getItem(i))
        df_03 = df_03.withColumn(date, F.col('Dates').getItem(i))
        if (i < 2):
            df_02 = df_02.withColumn(seq, F.col('Sequence').getItem(i))
            df_02 = df_02.withColumn(date, F.col('Dates').getItem(i))



for i in range(4):
   j = i + 1
   seq = 'Seq_{}'.format(j)
   month = 'Month_{}'.format(j)
   year = 'Year_{}'.format(j)
   df_04= df_04.withColumn(seq, F.col('Sequence').getItem(i))
   df_04 = df_04.withColumn(month, F.col('Months').getItem(i))
   df_04 = df_04.withColumn(year, F.col('Years').getItem(i))
   if (i < 3):
      df_03 = df_03.withColumn(seq, F.col('Sequence').getItem(i))
      df_03 = df_03.withColumn(month, F.col('Months').getItem(i))
      df_03 = df_03.withColumn(year, F.col('Years').getItem(i))
      if (i < 2):
         df_02 = df_02.withColumn(seq, F.col('Sequence').getItem(i))
         df_02 = df_02.withColumn(month, F.col('Months').getItem(i))
         df_02 = df_02.withColumn(year, F.col('Years').getItem(i))


for i in range(1,5):
   date = 'Date_{}'.format(i)
   month = 'Month_{}'.format(i)
   year = 'Year_{}'.format(i)
   df_04 = df_04.withColumn(date, (F.col(month)/12) + F.col(year))
   if (i < 4):
      df_03 = df_03.withColumn(date, (F.col(month)/12) + F.col(year))
      if (i < 3):
         df_02 = df_02.withColumn(date, (F.col(month)/12) + F.col(year))


for i in range(1,4):
   meta = 'Metachronous_{}'.format(i)
   date_2 = 'Date_{}'.format(i+1)
   date_1 = 'Date_{}'.format(i)
   df_04 = df_04.withColumn(meta, F.abs((F.col(date_2)-F.col(date_1))) >= 0.17)
   if (i < 3):
      df_03 = df_03.withColumn(meta, F.abs((F.col(date_2)-F.col(date_1))) >= 0.17)
      if (i < 2):
         df_02 = df_02.withColumn(meta, F.abs((F.col(date_2)-F.col(date_1))) >= 0.17)


df_02 = df_02.filter(F.col('Metachronous_1')==True)
df_03 = df_03.filter((F.col('Metachronous_1')==True)&(F.col('Metachronous_2')==True))
df_04 = df_04.filter((F.col('Metachronous_1')==True)&(F.col('Metachronous_2')==True)&(F.col('Metachronous_3')==True))

df_02 = df_02.filter(df_02.Seq_1 < df_02.Seq_2)
df_03 = df_03.filter(df_03.Seq_1 < df_03.Seq_2)
df_03 = df_03.filter(df_03.Seq_2 < df_03.Seq_3)
df_04 = df_04.filter(df_04.Seq_1 < df_04.Seq_2)
df_04 = df_04.filter(df_04.Seq_2 < df_04.Seq_3)
df_04 = df_04.filter(df_04.Seq_3 < df_04.Seq_4)

df_02 = df_02.drop('Month_1', 'Month_2', 'Year_1', 'Year_2', 'Date_1', 'Date_2')
df_03 = df_03.drop('Month_1', 'Month_2', 'Month_3', 'Year_1', 'Year_2', 'Year_3', 'Date_1', 'Date_2', 'Date_3')
df_04 = df_04.drop('Month_1', 'Month_2', 'Month_3', 'Month_4', 'Year_1', 'Year_2', 'Year_3', 'Year_4', 'Date_1', 'Date_2', 'Date_3', 'Date_4')

for i in range(1,5):
   seq = 'Seq_{}'.format(i)
   df_04 = df_04.withColumn(seq, F.lit(i))
   if (i < 4):
      df_03 = df_03.withColumn(seq, F.lit(i))
      if (i < 3):
         df_02 = df_02.withColumn(seq, F.lit(i))


for i in range(1,5):
   cancer = 'Cancer_{}'.format(i)
   seq = 'Seq_{}'.format(i)
   df_04 = df_04.withColumn(cancer, F.concat_ws('_', F.col('items').getItem(i-1), F.col(seq)))
   if (i < 4):
      df_03 = df_03.withColumn(cancer, F.concat_ws('_', F.col('items').getItem(i-1), F.col(seq)))
      if (i < 3):
         df_02 = df_02.withColumn(cancer, F.concat_ws('_', F.col('items').getItem(i-1), F.col(seq)))


df_02 = df_02.withColumn('items', F.array('Cancer_1', 'Cancer_2'))
df_03 = df_03.withColumn('items', F.array('Cancer_1', 'Cancer_2', 'Cancer_3'))
df_04 = df_04.withColumn('items', F.array('Cancer_1', 'Cancer_2', 'Cancer_3', 'Cancer_4'))
df_02 = df_02.select('Patient_ID', 'items', 'Sex', 'Ages')
df_03 = df_03.select('Patient_ID', 'items', 'Sex', 'Ages')
df_04 = df_04.select('Patient_ID', 'items', 'Sex', 'Ages')

data = df_02.union(df_03).union(df_04)
data = data_final
data = data.select('Patient_ID', 'items', 'Sex', 'Ages')

for i in range(4):
    j = i + 1
    level = 'Level_{}'.format(j)
    age = 'Age_{}'.format(j)
    data = data.withColumn(level, data.items.getItem(i))
    data = data.withColumn(age, data.Ages.getItem(i))

data = data.withColumn('Sex', F.col('Sex').getItem(0))

level_1 = data.select('Patient_ID', 'Level_1', 'Age_1', 'Sex')
level_2 = data.select('Patient_ID', 'Level_2', 'Age_2', 'Sex')
level_3 = data.select('Patient_ID', 'Level_3', 'Age_3', 'Sex')
level_4 = data.select('Patient_ID', 'Level_4', 'Age_4', 'Sex')

level_3 = level_3.withColumnRenamed('Level_3', 'Level')
level_3 = level_3.withColumnRenamed('Age_3', 'Age')
level_3 = level_3.dropna(how='any', subset='Level')
level_4 = level_4.withColumnRenamed('Level_4', 'Level')
level_4 = level_4.withColumnRenamed('Age_4', 'Age')
level_4 = level_4.dropna(how='any', subset='Level')
level_h = level_3.union(level_4)
level_h = level_h.withColumn('Level', F.split(F.col('Level'), '_'))
level_h = level_h.withColumn('Level', F.col('Level').getItem(0))

level_1.groupBy('Level_1', 'Sex').count().sort('count', ascending=False).show(150, truncate=False)
level_2.groupBy('Level_2', 'Sex').count().sort('count', ascending=False).show(150, truncate=False)
level_h.groupBy('Level', 'Sex').count().sort('count', ascending=False).show(150, truncate=False)


l1 = level_1.groupBy('Level_1').agg(F.min('Age_1').alias('Min'), F.max('Age_1').alias('Max'), F.mean('Age_1').alias('Mean'), F.count('Age_1').alias('Count'))
level_1.registerTempTable('l1Table')
l1_med = sqlContext.sql('select Level_1, percentile_approx(Age_1, 0.5) as Median from l1Table group by Level_1')
l1 = l1.join(l1_med, l1.Level_1==l1_med.Level_1, 'left')
l1 = l1.sort('Count', ascending=False)
l1.show()

l2 = level_2.groupBy('Level_2').agg(F.min('Age_2').alias('Min'), F.max('Age_2').alias('Max'), F.mean('Age_2').alias('Mean'), F.count('Age_2').alias('Count'))
level_2.registerTempTable('l2Table')
l2_med = sqlContext.sql('select Level_2, percentile_approx(Age_2, 0.5) as Median from l2Table group by Level_2')
l2 = l2.join(l2_med, l2.Level_2==l2_med.Level_2, 'left')
l2 = l2.sort('Count', ascending=False)
l2.show()

lh = level_h.groupBy('Level').agg(F.min('Age').alias('Min'), F.max('Age').alias('Max'), F.mean('Age').alias('Mean'), F.count('Age').alias('Count'))
level_h.registerTempTable('lhTable')
lh_med = sqlContext.sql('select Level, percentile_approx(Age, 0.5) as Median from lhTable group by Level')
lh = lh.join(lh_med, lh.Level==lh_med.Level, 'left')
lh = lh.sort('Count', ascending=False)
lh.show()



df2 = sqlContext.sql("select agent_id, percentile_approx(payment_amount,0.95) as approxQuantile from df group by agent_id")

from pyspark.ml.fpm import FPGrowth
fp = FPGrowth(minSupport=0.000015, minConfidence=0.001)

fpm = fp.fit(data_final)
fpm.freqItemsets.count()
fpm.freqItemsets.show(1363, truncate=False)
fpm.associationRules.count()
fpm.associationRules.show(219, truncate=False)

freqItems = fpm.freqItemsets
rules = fpm.associationRules

for i in range(3):
    j = i + 1
    ant = 'ant_{}'.format(j)
    rules = rules.withColumn(ant, F.col('antecedent').getItem(i))
    if (i==0):
        rules = rules.withColumn('con', F.col('consequent').getItem(i))



rules_breast_1 = rules.filter((F.array_contains(F.col('antecedent'), 'Breast_1'))|(F.array_contains(F.col('consequent'), 'Breast_1')))
rules_breast_2 = rules.filter((F.array_contains(F.col('antecedent'), 'Breast_2'))|(F.array_contains(F.col('consequent'), 'Breast_2')))
rules_breast_3 = rules.filter((F.array_contains(F.col('antecedent'), 'Breast_3'))|(F.array_contains(F.col('consequent'), 'Breast_3')))
rules_breast_4 = rules.filter((F.array_contains(F.col('antecedent'), 'Breast_4'))|(F.array_contains(F.col('consequent'), 'Breast_4')))

rules_br = rules_breast_1.union(rules_breast_2).union(rules_breast_3).union(rules_breast_4)

rules_prostate_1 = rules.filter((F.array_contains(F.col('antecedent'), 'Prostate_1'))|(F.array_contains(F.col('consequent'), 'Prostate_1')))
rules_prostate_2 = rules.filter((F.array_contains(F.col('antecedent'), 'Prostate_2'))|(F.array_contains(F.col('consequent'), 'Prostate_2')))
rules_prostate_3 = rules.filter((F.array_contains(F.col('antecedent'), 'Prostate_3'))|(F.array_contains(F.col('consequent'), 'Prostate_3')))
rules_prostate_4 = rules.filter((F.array_contains(F.col('antecedent'), 'Prostate_4'))|(F.array_contains(F.col('consequent'), 'Prostate_4')))

rules_pr = rules_prostate_1.union(rules_prostate_2).union(rules_prostate_3).union(rules_prostate_4)

rules_lung_1 = rules.filter((F.array_contains(F.col('antecedent'), 'Lung_1'))|(F.array_contains(F.col('consequent'), 'Lung_1')))
rules_lung_2 = rules.filter((F.array_contains(F.col('antecedent'), 'Lung_2'))|(F.array_contains(F.col('consequent'), 'Lung_2')))
rules_lung_3 = rules.filter((F.array_contains(F.col('antecedent'), 'Lung_3'))|(F.array_contains(F.col('consequent'), 'Lung_3')))
rules_lung_4 = rules.filter((F.array_contains(F.col('antecedent'), 'Lung_4'))|(F.array_contains(F.col('consequent'), 'Lung_4')))

rules_lu = rules_lung_1.union(rules_lung_2).union(rules_lung_3).union(rules_lung_4)

rules_colon_1 = rules.filter((F.array_contains(F.col('antecedent'), 'Colon_1'))|(F.array_contains(F.col('consequent'), 'Colon_1')))
rules_colon_2 = rules.filter((F.array_contains(F.col('antecedent'), 'Colon_2'))|(F.array_contains(F.col('consequent'), 'Colon_2')))
rules_colon_3 = rules.filter((F.array_contains(F.col('antecedent'), 'Colon_3'))|(F.array_contains(F.col('consequent'), 'Colon_3')))
rules_colon_4 = rules.filter((F.array_contains(F.col('antecedent'), 'Colon_4'))|(F.array_contains(F.col('consequent'), 'Colon_4')))

rules_co = rules_colon_1.union(rules_colon_2).union(rules_colon_3).union(rules_colon_4)

rules_lymphoma_1 = rules.filter((F.array_contains(F.col('antecedent'), 'Lymphoma_1'))|(F.array_contains(F.col('consequent'), 'Lymphoma_1')))
rules_lymphoma_2 = rules.filter((F.array_contains(F.col('antecedent'), 'Lymphoma_2'))|(F.array_contains(F.col('consequent'), 'Lymphoma_2')))
rules_lymphoma_3 = rules.filter((F.array_contains(F.col('antecedent'), 'Lymphoma_3'))|(F.array_contains(F.col('consequent'), 'Lymphoma_3')))
rules_lymphoma_4 = rules.filter((F.array_contains(F.col('antecedent'), 'Lymphoma_4'))|(F.array_contains(F.col('consequent'), 'Lymphoma_4')))

rules_ly = rules_lymphoma_1.union(rules_lymphoma_2).union(rules_lymphoma_3).union(rules_lymphoma_4)

import pandas as np
rules_br.toPandas().to_csv('/tmp/rules_br.csv')
rules_pr.toPandas().to_csv('/tmp/rules_pr.csv')
rules_lu.toPandas().to_csv('/tmp/rules_lu.csv')
rules_co.toPandas().to_csv('/tmp/rules_co.csv')
rules_ly.toPandas().to_csv('/tmp/rules_ly.csv')