# Employees Inside Office (Part 2)
  
Questions: A company record its employee's movement In and Out of office in a table. Please note below points about the data:
           1- First entry for each employee is “in”
           2- Every “in” is succeeded by an “out”
           3- Employee can work across days

Write an SQL to measure the time spent by each employee inside the office between “2019-04-01 14:00:00” and “2019-04-02 10:00:00" in minutes,
display the output in ascending order of employee id .

Columns are  Emp_id, action,createddate

# Difficulty Level: HARD

----------------------Creation of Data----------------------------------------------
CREATE TABLE emp_in_out_records(
          emp_id nvarchar(10),
          action nvarchar(5),
          createddate datetime
          )

INSERT INTO   emp_in_out_records
VALUES(1,'IN','2019-04-01 12:00:00'),(1,'OUT','2019-04-01 15:00:00'),(1,'IN','2019-04-01 17:00:00'),(1,'OUT','2019-04-01 21:00:00')
          (2,'IN','2019-04-01 10:00:00'),(2,'OUT','2019-04-01 13:00:00'),(3,'IN','2019-04-01 19:00:00'),(3,'OUT','2019-04-02 05:00:00')
          (4,'IN','2019-04-01 18:00:00'),(4,'OUT','2019-04-02 20:00:00'),
  (5,'IN','2019-04-01 10:00:00'),(5,'OUT','2019-04-02 11:00:00'),
  (6,'IN','2019-04-02 11:00:00'),(6,'OUT','2019-04-02 16:00:00')

-----------------------Solution-----------------------------------------------------
WITH in_and_out_times AS
(
SELECT emp_id,action,createddate 'in_time',LEAD(createddate) OVER(PARTITION BY emp_id ORDER BY createddate) 'out_time' 
FROM emp_in_out_records
)
,
 considered_time AS (
 SELECT emp_id
 , CASE WHEN in_time < '2019-04-01 14:00:00' THEN '2019-04-01 14:00:00' ELSE in_time END AS  in_time
 , CASE WHEN out_time > '2019-04-02 10:00:00' THEN '2019-04-02 10:00:00' ELSE out_time END AS out_time
 FROM in_and_out_times
 WHERE action='in'
 )
 
 SELECT emp_id,sum(CASE WHEN in_time > out_time THEN 0 ELSE DATEDIFF(minute,in_time,out_time)  END ) 'InSidetime' 
 FROM  considered_time
 GROUP BY emp_id
 ORDER BY emp_id
------------------------
Use Lead and DateDiff functions also use case statement CTE wherever required.
