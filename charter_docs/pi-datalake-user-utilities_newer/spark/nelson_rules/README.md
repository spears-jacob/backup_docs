# Nelson Rules

## Description

This is a PySpark implementation of Nelson Rules for Anomaly Detection. The implementation is based on the following paper: [Nelson, B. L. (1984). "A graphical method for identifying outlying observations in experiments on industrial products". _Technometrics_. 26 (2): 177–181.](https://www.tandfonline.com/doi/abs/10.1080/00401706.1984.10487974)

There are 8 Nelson Rules in total. The rules are listed below:

1. One point is more than 3 standard deviations from the mean.
2. Nine (or more) points in a row are on the same side of the mean.
3. Six (or more) points in a row are continually increasing (or decreasing).
4. Fourteen (or more) points in a row alternate in direction, increasing then decreasing.
5. Two (or three) out of three points in a row are more than 2 standard deviations from the mean in the same direction.
6. Four (or five) out of five points in a row are more than 1 standard deviation from the mean in the same direction.
7. Fifteen points in a row are all within 1 standard deviation of the mean on either side of the mean.
8. Eight points in a row exist, but none within 1 standard deviation of the mean, and the points are in both directions from the mean.

We decided to add a new rule to the list:

9. Any point is a Null value, which means there is no data for that point.

## Usage

The Nelson Rules are implemented as a PySpark UDF. The UDF takes in a list of values and returns a list of boolean values indicating whether each rule is violated or not. The UDF can be used in a PySpark DataFrame as follows:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, BooleanType, DoubleType

from nelson_rules import nelson_udf

spark = SparkSession.builder.getOrCreate()

# Create a sample DataFrame
df = spark.createDataFrame()

# Create a UDF for the Nelson Rules
nelson_udf = udf(nelson_udf, ArrayType(BooleanType()))

# Apply the UDF to the DataFrame
df = df.withColumn('nelson_rules', nelson_udf(df['values']))
```

## Requirements

- Python 3.7+
- PySpark 3.3.0+
- Pandas 2.0+
- Pendulum 2.0+

Python and Spark is already installed in our EMR cluster. Is recommended to use emr version 6.8.0 or higher.

## Installation

The requirements can be installed using the following command:

```bash
pip install -r requirements.txt
```
Requirements text contains the following packages:

- pandas==2.0
- pendulum==2.0
- pyarrow==12.0.1

Hydra is used to manage the configuration files. Pandas is used to convert the Spark DataFrame to a Pandas DataFrame. Pendulum is package for easier time/date manipulation. PyArrow is used to convert the Pandas DataFrame back to a Spark DataFrame.