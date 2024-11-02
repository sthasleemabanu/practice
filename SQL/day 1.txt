# Employees Inside Office (Part 1)

Questions: A company record its employee's movement In and Out of office in a table. Please note below points about the data:
          1- First entry for each employee is “in”
          2- Every “in” is succeeded by an “out”
          3- Employee can work across days

Write a SQL to find the number of employees inside the Office at “2019-04-01 19:05:00"
          Columns are  Emp_id, action,createddate

# Difficulty Level: EASY

----------------------Creation of Data----------------------------------------------
CREATE TABLE emp_in_out_records(
          emp_id nvarchar(10),
          action nvarchar(5),
          createddate datetime
          )

INSERT INTO   emp_in_out_records
VALUES(1,'IN','2019-04-01 12:00:00'),(1,'OUT','2019-04-01 15:00:00'),(1,'IN','2019-04-01 17:00:00'),(1,'OUT','2019-04-01 21:00:00')
          (2,'IN','2019-04-01 10:00:00'),(2,'OUT','2019-04-01 16:00:00'),(3,'IN','2019-04-01 19:00:00'),(3,'OUT','2019-04-02 05:00:00')
          (4,'IN','2019-04-01 10:00:00'),(4,'OUT','2019-04-01 20:00:00')

-----------------------Solution-----------------------------------------------------
Declare @time datetime ='2019-04-01 19:05:00'
WITH cte AS(
SELECT *,LEAD(createddate) OVER(PARTITION by emp_id ORDER BY emp_id) 'InTime' 
FROM emp_in_out_records
)

SELECT COUNT(emp_id) 'Count_Emp_id' 
FROM cte
WHERE @time BETWEEN createddate AND InTime
AND action='IN'

-----LEAD---
SQL LEAD function is a SQL window function which allows you to access data from a subsequent row and compare it to the current row. 
          This is especially useful when you need to work with sequences of data.
          For example, if you're looking at sales data, LEAD can show you tomorrow's sales right next to today's, all in one row. 
          This makes it easy to see changes or trends right away.
Refer: https://learnsql.com/blog/sql-lead-function/#what-is-sql-lead-function
