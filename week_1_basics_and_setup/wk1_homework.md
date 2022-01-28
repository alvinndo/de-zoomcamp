## Alvin Do
## 23 January 2022

#

# Question 3: Count records
### How many taxi trips were there on January 15?

```
SELECT
	COUNT(*)
FROM
	yellow_taxi_trips
WHERE
	DATE(tpep_pickup_datetime) = '2021-01-15'
```

# Question 4: Largest tip for each day
### On which day it was the largest tip in January? (note: it's not a typo, it's "tip", not "trip")

```
SELECT
	*
FROM
	yellow_taxi_trips
ORDER BY
	tip_amount DESC
LIMIT 1;
```

# Question 5: Most popular destination
### What was the most popular destination for passengers picked up in central park on January 14? Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

```
SELECT 
	zone_do."Zone",
	COUNT(zone_do."Zone")
FROM
	yellow_taxi_trips AS trips
INNER JOIN
	zones AS zone_pu
ON 
	trips."PULocationID" = zone_pu."LocationID"
INNER JOIN
	zones AS zone_do
ON
	trips."DOLocationID" = zone_do."LocationID"
WHERE
	zone_pu."Zone" ILIKE 'Central Park'
GROUP BY
	zone_do."Zone"
ORDER BY
	count(zone_do."Zone") DESC
LIMIT 1;
```

# Question 6: Most expensive route
### What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)? Enter two zone names separated by a slash.
### For example:"Jamaica Bay / Clinton East"If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

```
SELECT
	CONCAT(zone_pu."Zone", ' / ', zone_do."Zone") AS pickup_dropoff_pair,
	AVG(total_amount)
FROM 
	yellow_taxi_trips AS trips
INNER JOIN
	zones AS zone_pu
ON
	trips."PULocationID" = zone_pu."LocationID"
INNER JOIN
	zones AS zone_do
ON
	trips."DOLocationID" = zone_do."LocationID"
GROUP BY
	CONCAT(zone_pu."Zone", ' / ', zone_do."Zone")
ORDER BY
	AVG(total_amount) DESC
LIMIT 1;
```