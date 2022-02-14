## Alvin Do
## 13 February 2022

#

## Question 1:
### What is count for fhv vehicles data for year 2019

```
SELECT
	COUNT(*)
FROM
	`sage-now-339201.ny_taxi.fhv_data_external`
```

## Question 2:
### How many distinct dispatching_base_num we have in fhv for 2019 

```
SELECT
	COUNT(DISTINCT(dispatching_base_num))
FROM
	`sage-now-339201.ny_taxi.fhv_data_external`
```

## Question 4:
### What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279

```
SELECT 
    COUNT(*) 
FROM 
    `sage-now-339201.ny_taxi.fhv_data_external`
WHERE 
    (dispatching_base_num IN ('B00987', 'B02060', 'B02279')) AND 
    (DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31')
```
