#!/bin/bash

# Number of iterations
populate_hive=$1

if [ "$populate_hive" == "populate" ]; then
  hive <<EOF


CREATE TABLE delay_flights (
     Id INT,
     Year INT,
     Month INT,
     DayofMonth INT,
     DayOfWeek INT,
     DepTime INT,
     CRSDepTime INT,
     ArrTime INT,
     CRSArrTime INT,
     UniqueCarrier STRING,
     FlightNum INT,
     TailNum STRING,
     ActualElapsedTime INT,
     CRSElapsedTime INT,
     AirTime INT,
     ArrDelay DOUBLE,
     DepDelay DOUBLE,
     Origin STRING,
     Dest STRING,
     Distance INT,
     TaxiIn INT,
     TaxiOut INT,
     Cancelled INT,
     CancellationCode STRING,
     Diverted DOUBLE,
     CarrierDelay INT,
     WeatherDelay INT,
     NASDelay INT,
     SecurityDelay INT,
     LateAircraftDelay INT
     ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://airline-data-uom/new_hivesession';


LOAD DATA INPATH 's3://airline-data-uom/input/DelayedFlights-updated.csv' OVERWRITE INTO TABLE delay_flights;

EOF

fi

# Hive queries
queries=("SELECT Year, avg((CarrierDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;"
  "SELECT Year, avg((NASDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;"
  "SELECT Year, avg((WeatherDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;"
  "SELECT Year, avg((LateAircraftDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;"
  "SELECT Year, avg((SecurityDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;"
  # Add more queries as needed
)

execute_query() {
  local query=$1
  hive <<EOF
    ${query}
EOF
  sleep 2
  hive <<EOF
    ${query}
EOF
  sleep 2
  hive <<EOF
    ${query}
EOF
  sleep 2
  hive <<EOF
    ${query}
EOF
  sleep 2
  hive <<EOF
    ${query}
EOF

}

for query in "${queries[@]}"; do
  execute_query "$query"
  sleep 2
done
