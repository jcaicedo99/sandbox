from pyspark.sql import SparkSession


# Usage of config()
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()
      
print(spark.catalog.listDatabases())      

command = 'go north'
# command = 'drop ax shield'
match command.split():
    case ["help"]:
        print("""You can use the following commands:
        """)
    case ["go", direction]: 
        print(direction)
    case ["drop", *objects]:
        print(objects)
    # Other cases
    case _:
        print(f"Sorry, I couldn't understand {command!r}")  