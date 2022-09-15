WITH 

END_TIME AS

  (SELECT WAREHOUSE_NAME,
          TIMESTAMP,
          event_name,
          row_number() OVER (
                             PARTITION BY WAREHOUSE_NAME  ORDER BY TIMESTAMP ASC) RANK
   FROM snowflake.account_usage.warehouse_events_history
   WHERE event_name in ('SUSPEND_WAREHOUSE')
     AND event_state='COMPLETED' --AND TO_DATE(TIMESTAMP)='2021-12-08'
   ORDER BY WAREHOUSE_NAME,TIMESTAMP ASC),
   
   
START_TIME AS

  (SELECT WAREHOUSE_NAME,
          TIMESTAMP,
          EVENT_NAME,
          RANK
   FROM
     (SELECT WAREHOUSE_NAME,
             TIMESTAMP,
             event_name,
             row_number() OVER (
                               PARTITION BY WAREHOUSE_NAME ORDER BY TIMESTAMP ASC) RANK
      FROM snowflake.account_usage.warehouse_events_history
      WHERE event_name in (
                           'RESUME_WAREHOUSE')
        AND event_state='COMPLETED' --AND TO_DATE(TIMESTAMP)='2021-12-09'
      ORDER BY WAREHOUSE_NAME,TIMESTAMP ASC))
	  
	  
	  
SELECT ROUND(SUM(MINUTES)/60, 2) AS TOTAL_MINUTES
FROM
  (SELECT WAREHOUSE_NAME,
          START_TIME,
          END_TIME,
          MINUTES,RANK
   FROM
     (SELECT WAREHOUSE_NAME,
             START_TIME,
             END_TIME,
             MINUTES,
             ROW_NUMBER() OVER (PARTITION BY WAREHOUSE_NAME
                                ORDER BY START_TIME ASC) RANK
      FROM
        (SELECT A.WAREHOUSE_NAME,
                A.TIMESTAMP START_TIME,
                B.TIMESTAMP END_TIME,
                datediff(MINUTE, A.TIMESTAMP, B.TIMESTAMP) MINUTES
         FROM START_TIME A
         INNER JOIN END_TIME B ON A.RANK=B.RANK AND A.WAREHOUSE_NAME=B.WAREHOUSE_NAME AND TO_DATE(A.TIMESTAMP)=TO_DATE(B.TIMESTAMP)))
   )

WHERE WAREHOUSE_NAME=:Warehouse AND START_TIME =:daterange

--WHERE MOD(RANK, 2)=0

