import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, to_date, month, year, round, substring, lit, get_json_object, explode, sum
from utils.utility import load_config
from datetime import datetime

class BatchProcessor:
    def __init__(self, config_file='config.json'):
        self.config = load_config(config_file)
        self.spark = self.setup_spark_session()
        self.kitchen_df = None
        self.dispatch_df = None
        self.dlq_df = None
        self.batch_processed_path = self.config.get("batch_processed_path", "./output/batch_processed_data")

    def setup_spark_session(self):
        session = (SparkSession.builder
            .appName("CentralKitchenPipeline")
            .getOrCreate())
        session.sparkContext.setLogLevel("WARN")
        return session

    def load_all_data(self):
        print("Loading data into memory...")
        
        kitchen_path = self.config.get("kitchen_data_path", "./output/kitchen_data")
        dispatch_path = self.config.get("dispatch_data_path", "./output/dispatch_data")
        dlq_path = self.config.get("dlq_data_path", "./output/dlq_data")

        try:
            raw_kitchen = self.spark.read.parquet(kitchen_path)
            self.kitchen_df = (raw_kitchen
                .withColumn("event_date", to_date(substring(col("event_timestamp").cast("string"), 1, 10), 'yyyy-MM-dd')))

            raw_dispatch = self.spark.read.parquet(dispatch_path)
            self.dispatch_df = (raw_dispatch
                .withColumn("event_date", to_date(substring(col("event_timestamp").cast("string"), 1, 10), 'yyyy-MM-dd')))

            raw_dlq = self.spark.read.parquet(dlq_path)
            
            self.dlq_df = (raw_dlq
                .withColumn("payload_timestamp", get_json_object(col("raw_message"), "$.payload.event_timestamp"))
                .withColumn("event_id", get_json_object(col("raw_message"), "$.metadata.event_id"))
                .withColumn("batch_id", get_json_object(col("raw_message"), "$.payload.batch_id"))
                .withColumn("error_reason", 
                    when(~col("raw_message").startswith("{"), "Invalid JSON String")
                    .when(get_json_object(col("raw_message"), "$.metadata").isNull(), "Missing Metadata")
                    .when(get_json_object(col("raw_message"), "$.metadata.event_id").isNull(), "Missing Event ID")
                    .when(get_json_object(col("raw_message"), "$.metadata.event_type").isNull(), "Missing Event Type")
                    .when(get_json_object(col("raw_message"), "$.payload").isNull(), "Missing Payload")
                    .when(get_json_object(col("raw_message"), "$.payload.batch_id").isNull(), "Missing Batch ID")
                    .when(get_json_object(col("raw_message"), "$.payload.action").isNull(), "Missing Batch Action")
                    .otherwise("Others")
                )
                .withColumn("event_date", to_date(substring(col("payload_timestamp"), 1, 10), 'yyyy-MM-dd')))

            print("Data loaded successfully.")
            return True
        except Exception as error:
            print(f"Failed to load data. Error: {error}")
            return False

    def display_daily_event(self, target_date, row_limit):
        if not self.kitchen_df:
            print("Please run load_all_data() first.")
            return

        print(f"\n--- DAILY EVENTS FOR {target_date} ---")

        daily_kitchen = self.kitchen_df.filter(col("event_date") == lit(target_date).cast("date"))
        daily_dispatch = self.dispatch_df.filter(col("event_date") == lit(target_date).cast("date"))
        
        kitchen_count = daily_kitchen.count()
        dispatch_count = daily_dispatch.count()

        print(f"\nKitchen Events: {kitchen_count}")
        print(f"Dispatch Events: {dispatch_count}")

        if kitchen_count > 0:
            print("\nDaily Kitchen Data:")
            (daily_kitchen
             .select("event_timestamp", "batch_id", "recipe_id", "station_id", "action", 
                     "temperature_celsius", "weight_kg", "temperature_status", "weight_status", "sensor_status")
             .orderBy("event_timestamp").show(row_limit, truncate=False))

        if dispatch_count > 0:
            print("\nDaily Dispatch Data:")
            (daily_dispatch
             .select("event_timestamp", "batch_id", "driver_id", "canteen_id", "action", 
                     "truck_temp_celsius", "temperature_status", "sensor_status")
             .orderBy("event_timestamp").show(row_limit, truncate=False))

    def display_daily_report(self, target_date, row_limit):
        if not self.kitchen_df:
            print("Please run load_all_data() first.")
            return

        print(f"\n--- DAILY REPORT FOR {target_date} ---")

        daily_kitchen = self.kitchen_df.filter(col("event_date") == lit(target_date).cast("date"))
        daily_dispatch = self.dispatch_df.filter(col("event_date") == lit(target_date).cast("date"))
        all_batches_daily = daily_kitchen.select("batch_id").union(daily_dispatch.select("batch_id"))
        total_batches_daily = all_batches_daily.distinct().count()

        print(f"\nTotal Batches Processed: {total_batches_daily}")

        print("\nDaily Kitchen Station Performance:")
        station_report = daily_kitchen.groupBy("station_id").agg(
            count(when(col("action").isin("COOKING", "PREPARING", "PACKING"), True)).alias("total_times_active"),
            count(when(col("temperature_status") != "OK", True)).alias("total_temp_warnings"),
            count(when(col("weight_status") != "OK", True)).alias("total_weight_warnings"),
            count(when(col("sensor_status") != "OK", True)).alias("total_sensor_warnings")
        ).orderBy("station_id")
        station_report.show(row_limit, truncate=False)
        station_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/daily/daily_station_{target_date}")
        
        print("\nDaily Recipe Performance:")
        recipe_report = daily_kitchen.groupBy("recipe_id").agg(
            count(when(col("action") == "COOKING", True)).alias("total_times_cooked")
        ).orderBy("recipe_id")
        recipe_report.show(row_limit, truncate=False)
        recipe_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/daily/daily_recipe_{target_date}")
        
        print("\nDaily Ingredient Usage (kg):")
        ingredient_report = (daily_kitchen.dropDuplicates(["batch_id"])
         .select("recipe_id", explode("ingredients").alias("ingredients"))
         .groupBy("recipe_id", "ingredients.item")
         .agg(
             round(sum(col("ingredients.amount_kg").cast("double")), 2).alias("total_weight_used")
         )
         .orderBy("total_weight_used", ascending=False))
        ingredient_report.show(row_limit, truncate=False)
        ingredient_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/daily/daily_ingredient_{target_date}")

        print("\nDaily Driver Performance:")
        driver_report = daily_dispatch.groupBy("driver_id").agg(
            count(when(col("action") == "DELIVERED", True)).alias("total_deliveries"),
            count(when(col("temperature_status") != "OK", True)).alias("total_truck_temp_warnings"),
            count(when(col("sensor_status") != "OK", True)).alias("total_sensor_warnings")
        ).orderBy("driver_id")
        driver_report.show(row_limit, truncate=False)
        driver_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/daily/daily_driver_{target_date}")

        print("\nDaily Canteen Performance:")
        canteen_report = daily_dispatch.groupBy("canteen_id").agg(
            count(when(col("action") == "DELIVERED", True)).alias("total_deliveries")
        ).orderBy("canteen_id")
        canteen_report.show(row_limit, truncate=False)
        canteen_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/daily/daily_canteen_{target_date}")

    def display_daily_dlq(self, target_date, row_limit):
        if not self.dlq_df:
            return

        print(f"\n--- DAILY DLQ REPORT FOR {target_date} ---")

        daily_dlq = self.dlq_df.filter(col("event_date") == lit(target_date).cast("date"))
        dlq_count = daily_dlq.count()
        
        print(f"\nTotal Corrupted Data for {target_date}: {dlq_count} rows")
        if dlq_count > 0:
            dlq_report = daily_dlq.select("payload_timestamp", "event_id", "batch_id", "error_reason").orderBy("payload_timestamp")
            dlq_report.show(row_limit, truncate=False)
            dlq_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/daily/daily_dlq_{target_date}")

    def display_monthly_report(self, target_year, target_month, row_limit):
        if not self.kitchen_df:
            print("Please run load_all_data() first.")
            return

        month_str = f"{target_year}-{target_month:02d}"
        print(f"\n--- MONTHLY REPORT FOR {month_str} ---")

        month_kitchen = self.kitchen_df.filter((year(col("event_date")) == target_year) & (month(col("event_date")) == target_month))        
        month_dispatch = self.dispatch_df.filter((year(col("event_date")) == target_year) & (month(col("event_date")) == target_month))
        all_batches_month = month_kitchen.select("batch_id").union(month_dispatch.select("batch_id"))
        total_batches_month = all_batches_month.distinct().count()

        print(f"\nTotal Batches Processed: {total_batches_month}")

        print("\nMonthly Kitchen Station Performance:")
        station_month_report = month_kitchen.groupBy("station_id").agg(
            count(when(col("action").isin("COOKING", "PREPARING", "PACKING"), True)).alias("total_times_active"),
            count(when(col("temperature_status") != "OK", True)).alias("total_temp_warnings"),
            count(when(col("weight_status") != "OK", True)).alias("total_weight_warnings"),
            count(when(col("sensor_status") != "OK", True)).alias("total_sensor_warnings")
        ).orderBy("station_id")
        station_month_report.show(row_limit, truncate=False)
        station_month_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/monthly/monthly_station_{month_str}")
        
        print("\nMonthly Recipe Performance:")
        recipe_month_report = month_kitchen.groupBy("recipe_id").agg(
            count(when(col("action") == "COOKING", True)).alias("total_times_cooked")
        ).orderBy("recipe_id")
        recipe_month_report.show(row_limit, truncate=False)
        recipe_month_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/monthly/monthly_recipe_{month_str}")
        
        print("\nMonthly Ingredient Usage (kg):")
        ingredient_month_report = (month_kitchen.dropDuplicates(["batch_id"])
         .select("recipe_id", explode("ingredients").alias("ingredients"))
         .groupBy("recipe_id", "ingredients.item")
         .agg(
             round(sum(col("ingredients.amount_kg").cast("double")), 2).alias("total_weight_used")
         )
         .orderBy("total_weight_used", ascending=False))
        ingredient_month_report.show(row_limit, truncate=False)
        ingredient_month_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/monthly/monthly_ingredient_{month_str}")

        print("\nMonthly Driver Performance:")
        driver_month_report = month_dispatch.groupBy("driver_id").agg(
            count(when(col("action") == "DELIVERED", True)).alias("total_deliveries"),
            count(when(col("temperature_status") != "OK", True)).alias("total_truck_temp_warnings"),
            count(when(col("sensor_status") != "OK", True)).alias("total_sensor_warnings")
        ).orderBy("driver_id")
        driver_month_report.show(row_limit, truncate=False)
        driver_month_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/monthly/monthly_driver_{month_str}")

        print("\nMonthly Canteen Performance:")
        canteen_month_report = month_dispatch.groupBy("canteen_id").agg(
            count(when(col("action") == "DELIVERED", True)).alias("total_deliveries")
        ).orderBy("canteen_id")
        canteen_month_report.show(row_limit, truncate=False)
        canteen_month_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/monthly/monthly_canteen_{month_str}")

    def display_monthly_dlq(self, target_year, target_month, row_limit):
        if not self.dlq_df:
            return

        month_str = f"{target_year}-{target_month:02d}"
        print(f"\n--- MONTHLY DLQ REPORT FOR {month_str} ---")

        month_dlq = self.dlq_df.filter((year(col("event_date")) == target_year) & (month(col("event_date")) == target_month))
        total_errors = month_dlq.count()

        print(f"\nTotal Corrupted Data for the month: {total_errors} rows")

        if total_errors > 0:
            dlq_month_report = month_dlq.select("payload_timestamp", "event_id", "batch_id", "error_reason").orderBy("payload_timestamp")
            dlq_month_report.show(row_limit, truncate=False)
            dlq_month_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/monthly/monthly_dlq_{month_str}")

        unknown_dlq = self.dlq_df.filter(col("event_date").isNull())
        unknown_dlq_count = unknown_dlq.count()

        print(f"\n--- DLQ REPORT FOR UNKNOWN TIMESTAMPS ---")
        
        print(f"\nTotal Corrupted Data with Unknown Timestamps: {unknown_dlq_count} rows")
        if unknown_dlq_count > 0:
            unknown_report = unknown_dlq.select("payload_timestamp", "event_id", "batch_id", "error_reason").orderBy("payload_timestamp")
            unknown_report.show(row_limit, truncate=False)
            unknown_report.write.mode("overwrite").option("header", "true").csv(f"{self.batch_processed_path}/unknown_timestamp_dlq")

    def run_tests(self):
        if not self.load_all_data():
            self.spark.stop()
            return

        while True:
            print("\n" + "="*30)
            print("         MAIN MENU")
            print("="*30)
            print("1. View Daily Events")
            print("2. View Daily Report")
            print("3. View Monthly Report")
            print("4. View DLQ Reports")
            print("5. View All Reports")
            print("0. Exit")
            
            choice = input("\nEnter your choice: ")

            if choice == '0':
                print("Exiting Batch Processor.")
                break

            if choice not in ['1', '2', '3', '4', '5']:
                print("Invalid choice. Please enter 1, 2, 3, 4, 5, or 0.")
                continue

            if choice in ['1', '2', '4', '5']:
                while True:
                    user_date = input("Enter the date you want to view (Format: YYYY-MM-DD): ")
                    try:
                        datetime.strptime(user_date, "%Y-%m-%d")
                        break 
                    except ValueError:
                        print("Error: The date must be in YYYY-MM-DD format. Please try again.")
                
                try:
                    user_rows = int(input("Enter the number of rows to display: "))
                except ValueError:
                    print("Invalid number entered. Defaulting to 5 rows.")
                    user_rows = 5

                target_year = int(user_date[0:4])
                target_month = int(user_date[5:7])

            elif choice == '3':
                while True:
                    user_month = input("Enter the year and month (Format: YYYY-MM): ")
                    try:
                        valid_date = datetime.strptime(user_month, "%Y-%m")
                        target_year = valid_date.year
                        target_month = valid_date.month
                        break
                    except ValueError:
                        print("Error: The format must be YYYY-MM. Please try again.")

                try:
                    user_rows = int(input("Enter the number of rows to display: "))
                except ValueError:
                    print("Invalid number entered. Defaulting to 5 rows.")
                    user_rows = 5

            if choice == '1':
                self.display_daily_event(user_date, user_rows)
                input("Press Enter to return to Main Menu...")
                
            elif choice == '2':
                self.display_daily_report(user_date, user_rows)
                input("Press Enter to return to Main Menu...")

            elif choice == '3':
                self.display_monthly_report(target_year, target_month, user_rows)
                input("Press Enter to return to Main Menu...")
                
            elif choice == '4':
                self.display_daily_dlq(user_date, user_rows)
                self.display_monthly_dlq(target_year, target_month, user_rows)
                input("Press Enter to return to Main Menu...")

            elif choice == '5':
                self.display_daily_report(user_date, user_rows)
                self.display_monthly_report(target_year, target_month, user_rows)
                self.display_daily_dlq(user_date, user_rows)
                self.display_monthly_dlq(target_year, target_month, user_rows)
                input("Press Enter to return to Main Menu...")
                
        print("Closing Spark Session...")
        self.spark.stop()