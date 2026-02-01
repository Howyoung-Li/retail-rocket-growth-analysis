from pyspark.sql import SparkSession, functions as F

def main():
	spark = (
		SparkSession.builder
		.appName("rr_growth_metrics")
		.getOrCreate()
	)

	# 1) Load raw events
	# RetailRocket events file:
	# timestamp, visitorid, event, itemid, transactionid

	events_path = "data/raw/events.csv"
	df = spark.read.option("header", True).csv(events_path)

	# 2) Basic cleaning + canonical schema
	# timestamp is in milliseconds in  RetailRocket
	df = (
		df
		.withColumn("ts_ms", F.col("timestamp").cast("long"))
		.withColumn("event_ts", F.to_timestamp((F.col("ts_ms")/1000).cast("double")))
		.withColumn("dt", F.to_date("event_ts"))
		.withColumn("user_id", F.col("visitorid").cast("string"))
		.withColumn("item_id", F.col("itemid").cast("string"))
		.withColumn("event_name", F.lower(F.col("event")))
		.withColumn("transaction_id", F.col("transactionid").cast("string"))
		.select("dt", "user_id", "item_id", "event_name", "transaction_id")
		.filter(F.col("dt").isNotNull())
	)

	# cache for repeated aggregation
	df.cache()

	# 3) daily overview metrics
	daily = (
		df.groupBy("dt")
		.agg(
			F.countDistinct("user_id").alias("dau"),
			F.countDistinct(F.when(F.col("event_name") == "view", F.col("user_id"))).alias("view_uv"),
			F.countDistinct(F.when(F.col("event_name") == "addtocart", F.col("user_id"))).alias("atc_uv"),
			F.countDistinct(F.when(F.col("event_name") == "transaction", F.col("user_id"))).alias("buy_uv"),
			F.sum(F.when(F.col("event_name") == "view", 1).otherwise(0)).alias("view_cnt"),
			F.sum(F.when(F.col("event_name") == "addtocart", 1).otherwise(0)).alias("atc_cnt"),
			F.sum(F.when(F.col("event_name") == "transaction", 1).otherwise(0)).alias("buy_cnt"),
		)
		.withColumn("act_rate_uv", F.when(F.col("view_uv") > 0, F.col("atc_uv") / F.col("view_uv")))
		.withColumn("purchase_rate_uv", F.when(F.col("view_uv") > 0, F.col("buy_uv") / F.col("view_uv")))
		.withColumn("atc_to_buy_rate_uv", F.when(F.col("atc_uv") > 0, F.col("buy_uv") / F.col("atc_uv")))
		.orderBy("dt")
	)

	 # 4)  Funnel table (long format)
	funnel = (
		daily.select(
			"dt",
			F.lit("view").alias("step"), F.col("view_uv").alias("uv")
		)
		.unionByName(daily.select("dt", F.lit("addtocart").alias("step"), F.col("atc_uv").alias("uv")))
		.unionByName(daily.select("dt", F.lit("transaction").alias("step"), F.col("buy_uv").alias("uv")))
		.orderBy("dt", "step")
	)

	# 5) Top item daily (growth levers: traffic vs conversion items)
	top_items = (
		df.groupBy("dt", "item_id")
		.agg(
			F.countDistinct(F.when(F.col("event_name") == "view", F.col("user_id"))).alias("view_uv"),
			F.countDistinct(F.when(F.col("event_name") == "addtocart", F.col("user_id"))).alias("atc_uv"),
			F.countDistinct(F.when(F.col("event_name") == "transaction", F.col("user_id"))).alias("buy_uv"),
			F.sum(F.when(F.col("event_name") == "transaction", 1).otherwise(0)).alias("buy_cnt"),			
		)
	)

	# 6) simple cohort retention: users who return on D+1/ D+7 after first day
	first_day = df.groupBy("user_id").agg(F.min("dt").alias("cohort_dt"))
	active = df.select("user_id", "dt").distinct()

	retention = (
		first_day.join(active, on="user_id", how="left")
		.groupBy("cohort_dt")
		.agg(
			F.countDistinct("user_id").alias("cohort_size"),
			F.countDistinct(F.when(F.col("dt") == F.date_add(F.col("cohort_dt"), 1),F.col("user_id"))).alias("d1"),
			F.countDistinct(
				F.when(
					(F.col("dt") >= F.date_add(F.col("cohort_dt"), 1)) &
					(F.col("dt") <= F.date_add(F.col("cohort_dt"), 7)),
					F.col("user_id")
				)
			).alias("d7_rolling")
		)
		.withColumn("d1_retention", F.when(F.col("cohort_size") > 0, F.col("d1") / F.col("cohort_size")))
		.withColumn("d7_rolling_retention", F.when(F.col("cohort_size") > 0, F.col("d7_rolling") / F.col("cohort_size")))
		.orderBy("cohort_dt")
	)

	# 7) write outputs (csv for power BI)
	def write_csv(df_out, name: str):
		(df_out.write.mode("overwrite").option("header", True).csv(f"data/out/{name}"))

	
	write_csv(daily, "metrics_daily_overview")
	write_csv(funnel, "metrics_daily_funnel")
	write_csv(top_items, "metrics_top_items_daily")
	write_csv(retention, "metrics_cohort_retention")

	spark.stop()

if __name__=="__main__":
	main()