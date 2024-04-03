from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').getOrCreate()

products = spark.createDataFrame((
    (1, 'Product A'),
    (2, 'Product B'),
    (3, 'Product C'),
), ('product_id', 'product_name'))

categories = spark.createDataFrame((
    (1, 'Category A'),
    (2, 'Category B'),
    (3, 'Category C'),
), ('category_id', 'category_name'))

relation = spark.createDataFrame((
    (1, 1),
    (2, 1),
    (2, 2),
), ('product_id', 'category_id'))

relation.join(products, 'product_id', 'right') \
    .join(categories, 'category_id', 'left') \
    .select('product_name', 'category_name') \
    .sort('product_name', 'category_name') \
    .show()
