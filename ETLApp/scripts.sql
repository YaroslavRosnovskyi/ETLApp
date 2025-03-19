--Step 4 examples
SELECT TOP 1 PULocationID, AVG(tip_amount) AS avg_tip
FROM TripData
GROUP BY PULocationID
ORDER BY avg_tip DESC;

SELECT TOP 100 *
FROM TripData
ORDER BY trip_distance DESC;

SELECT TOP 100 *, DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration
FROM TripData
ORDER BY trip_duration DESC;

SELECT * 
FROM TripData 
WHERE PULocationID = 123
ORDER BY tpep_pickup_datetime DESC;

--Database creation
DROP TABLE IF EXISTS TripData;                              
    
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'TripData') AND type = 'U')
CREATE TABLE TripData (
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INT,
    trip_distance DECIMAL(10, 2),
    store_and_fwd_flag VARCHAR(3),
    PULocationID INT,
    DOLocationID INT,
    fare_amount DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2)
);