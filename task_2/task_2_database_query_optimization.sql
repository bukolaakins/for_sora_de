/*
Author : Olubukunola Akinsola <oo.akinsola@gmail.com>
Date   : 2024-10-24
Purpose: Sora Union DE Assessment - Task 2
SQL: PostgreSQL
*/

-- clickup table with partitioning by Date

CREATE TABLE clickup (
    id SERIAL,
    Name VARCHAR(255),
    hours INT,
    Date DATE
) PARTITION BY RANGE (Date);

-- partitions for clickup table for the years 2023 and 2024 created

CREATE TABLE clickup_2023 PARTITION OF clickup 
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE clickup_2024 PARTITION OF clickup 
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- created indexes on the clickup table to optimize queries

CREATE INDEX idx_clickup_name ON clickup(Name);
CREATE INDEX idx_clickup_hours ON clickup(hours);

-- float table creation
CREATE TABLE float (
    id SERIAL PRIMARY KEY,
    Name VARCHAR(255),
    Estimed_Hours INT,
    Role VARCHAR(255)
);

-- Created indexes on the float table to optimize queries
CREATE INDEX idx_float_name ON float(Name);
CREATE INDEX idx_float_estimed_hours ON float(Estimed_Hours);

-- Optimized Query

SELECT
    c.Name,
    f.Role,
    SUM(c.hours) AS Total_Tracked_Hours, -- added a comma after each column name
    SUM(f.Estimated_Hours) AS Total_Allocated_Hours, -- renamed column
    c.Date
FROM
    clickup c
JOIN
    float f ON c.Name = f.Name
GROUP BY
    c.Name, f.Role, c.Date  -- Included c.Date in the GROUP BY clause
HAVING
    SUM(c.hours) > 100
ORDER BY
    Total_Allocated_Hours DESC;
