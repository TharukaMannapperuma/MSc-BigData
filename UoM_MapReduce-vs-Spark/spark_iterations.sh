#!/bin/bash

SPARK_SHELL="/usr/bin/spark-shell"

QUERIES=(
  "SELECT Year, avg((CarrierDelay /ArrDelay)*100) as Year_wise_carrier_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC"
  "SELECT Year, avg((NASDelay /ArrDelay)*100) as Year_wise_NAS_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC"
  "SELECT Year, avg((WeatherDelay /ArrDelay)*100) as Year_wise_Weather_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC"
  "SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) as Year_wise_late_aircraft_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC"
  "SELECT Year, avg((SecurityDelay /ArrDelay)*100) as Year_wise_security_delay FROM delay_flights GROUP BY Year ORDER BY Year DESC"
)

execute_query_show() {
  local query="$1"
  $SPARK_SHELL <<EOF
    val df = spark.read.format("csv").option("header", "true").load("s3://airline-data-uom/input/DelayedFlights-updated.csv")
    df.createOrReplaceTempView("delay_flights")
    spark.time{spark.sql("$query").show()}
    spark.time{spark.sql("$query").show()}
    spark.time{spark.sql("$query").show()}
    spark.time{spark.sql("$query").show()}
    spark.time{spark.sql("$query").show()}

EOF
}

for query in "${QUERIES[@]}"; do
  execute_query_show "$query"
done
