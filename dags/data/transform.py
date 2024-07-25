from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split

# Initialize Spark session
spark = SparkSession.builder.appName("DataTransform").getOrCreate()

# Read the USPS data
zipcounty_df = spark.read.csv('gs://ca-registered-vehicles-raw_data_bucket/ZIP-COUNTY-FIPS_2021.csv', header=True)

# List of URLs for the vehicle fuel type data
data_urls = [
    'gs://ca-registered-vehicles-raw_data_bucket/0_vehicle-fuel-type-count-by-zip-code-20231.csv',
    'gs://ca-registered-vehicles-raw_data_bucket/1_vehicle-fuel-type-count-by-zip-code-2022.csv',
    'gs://ca-registered-vehicles-raw_data_bucket/2_vehicle-fuel-type-count-by-zip-code-2022.csv',
    'gs://ca-registered-vehicles-raw_data_bucket/3_vehicle-fuel-type-count-by-zip-code-2021.csv',
    'gs://ca-registered-vehicles-raw_data_bucket/4_vehicle-count-as-of-1-1-2020.csv',
    'gs://ca-registered-vehicles-raw_data_bucket/5_vehicle-fuel-type-count-by-zip-code.csv'
]

# Read all the vehicle fuel type data and combine into one DataFrame
fueltype_df = spark.read.csv(data_urls, header=True)

# Rename columns by replacing spaces with underscores
for col_name in fueltype_df.columns:
    fueltype_df = fueltype_df.withColumnRenamed(col_name, col_name.replace(' ', '_'))

# Fixing model year
fueltype_df = fueltype_df.withColumn('Model_Year', when(col('Model_Year') == '<2010', '2010').otherwise(col('Model_Year')))

# Fixing make names
fueltype_df = fueltype_df.withColumn('Make', when(col('Make') == 'UNKNOWN', 'OTHER/UNK')
                             .when(col('Make') == 'IC BUS LLC', 'ICBUS')
                             .when(col('Make') == 'KOVATCH MOBLIE EQUIPMENT CORP', 'KOVATCH')
                             .when(col('Make') == 'MOTOR COACH INDUSTRIES', 'MOTORCOACHIND')
                             .when((col('Make') == 'NORTH AMERICAN') | (col('Make') == 'NORTH AMERICAN BUS INDUSTRIES'), 'NABUSINDUST')
                             .when(col('Make') == 'THOMAS BUILT BUSES', 'THOMASBUS')
                             .when(col('Make') == 'WHITE/GMC', 'WHITE')
                             .otherwise(col('Make')))

# Remove spaces in make names
fueltype_df = fueltype_df.withColumn('Make', split(col('Make'), ' ').getItem(0))

# Drop rows where ZIP Code is 'OOS'
fueltype_df = fueltype_df.filter(col('ZIP_Code') != 'OOS')

# Convert ZIP_Code to integer
fueltype_df = fueltype_df.withColumn('ZIP_Code', col('ZIP_Code').cast('int'))

# Filter only the ZIPs in CA
cal_zip = zipcounty_df.filter(col('STATE') == 'CA')

# Create dictionary of all the zipcodes that belong to specific county
calizip_dict = cal_zip.rdd.map(lambda row: (row['ZIP'], row['COUNTYNAME'])).groupByKey().mapValues(list).collectAsMap()

# Broadcast the dictionary to optimize the join
calizip_dict_broadcast = spark.sparkContext.broadcast(calizip_dict)

# Map ZIP_Code to County using the broadcast dictionary
def map_zip_to_county(zip_code):
    return calizip_dict_broadcast.value.get(str(zip_code), ["Unknown"])[0]

# Register the UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
zip_to_county_udf = udf(map_zip_to_county, StringType())

# Apply the UDF
fueltype_df = fueltype_df.withColumn('County', zip_to_county_udf(col('ZIP_Code')))

# Write the transformed data to GCS
fueltype_df.write.csv('gs://ca-registered-vehicles-transformed_data_bucket/transformed_data/', header=True)

spark.stop()