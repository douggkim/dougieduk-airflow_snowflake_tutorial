WITH

-- Warehouse will give information about credit information.
-- WAREHOUSE_HISTORY view will calculate credit and cost grouped by warehouse name.

WAREHOUSE_HISTORY AS
  
(SELECT TO_DATE(START_TIME) START_TIME,
          WAREHOUSE_NAME,
          'SNOWFLAKE_WAREHOUSE_COST' CRITERIA,
                                     sum(credits_used) AS CREDITS,
                                     ROUND(sum(credits_used)*4, 2) COST
   FROM snowflake.account_usage.warehouse_metering_history
   WHERE to_date(start_time) >=date_trunc(MONTH, CURRENT_DATE)
     AND WAREHOUSE_NAME IS NOT NULL
     AND WAREHOUSE_NAME NOT IN (' ',
                                'CLOUD_SERVICES_ONLY')
   GROUP BY TO_DATE(START_TIME),
            WAREHOUSE_NAME),

-- Using the query execution time we will calculate the approx credits for the queries executed on warehouse.
-- We will compare the credits calculated from query execution with total credits from WAREHOUSE_HISTORY to calculate Idle cost.
			
			
QUERY_HISTORY AS
 
  (SELECT TO_DATE(START_TIME) START_TIME,
          WAREHOUSE_NAME,
          'QUERY_EXECUTION_COST' CRITERIA,
                                 sum(WAREHOUSE_COST) CREDITS,
                                 ROUND(sum(WAREHOUSE_COST) *4, 2) COST
   FROM
     (SELECT TO_DATE(START_TIME) START_TIME,
             WAREHOUSE_NAME,
             CASE
                 WHEN WAREHOUSE_SIZE='X-Small' THEN ((((EXECUTION_TIME/1000)/60))*(CLUSTER_NUMBER*1))/60
                 WHEN WAREHOUSE_SIZE='Small' THEN ((((EXECUTION_TIME/1000)/60))*(CLUSTER_NUMBER*2))/60
                 WHEN WAREHOUSE_SIZE='Medium' THEN ((((EXECUTION_TIME/1000)/60))*(CLUSTER_NUMBER*4))/60
                 WHEN WAREHOUSE_SIZE='Large' THEN ((((EXECUTION_TIME/1000)/60))*(CLUSTER_NUMBER*8))/60
                 WHEN WAREHOUSE_SIZE='X-Large' THEN ((((EXECUTION_TIME/1000)/60))*(CLUSTER_NUMBER*16))/60
                 WHEN WAREHOUSE_SIZE='2X-Large' THEN ((((EXECUTION_TIME/1000)/60))*(CLUSTER_NUMBER*32))/60
                 WHEN WAREHOUSE_SIZE='3X-Large' THEN ((((EXECUTION_TIME/1000)/60))*(CLUSTER_NUMBER*64))/60
                 WHEN WAREHOUSE_SIZE='4X-Large' THEN ((((EXECUTION_TIME/1000)/60))*(CLUSTER_NUMBER*128))/60
             END WAREHOUSE_COST
      FROM snowflake.account_usage.query_history
      WHERE to_date(start_time) >=date_trunc(MONTH, CURRENT_DATE) )
   GROUP BY TO_DATE(START_TIME),
            WAREHOUSE_NAME),
			
-- Calculate approx Idle credits and cost. This will use the calculation done from above steps.
			
 IDLE_COST AS
  
  (SELECT A.START_TIME,
          A.WAREHOUSE_NAME,
          'IDLE_COST' CRITERIA,
                      (A.CREDITS-B.CREDITS) CREDITS,
                      (A.CREDITS-B.CREDITS)*4 COST
   FROM WAREHOUSE_HISTORY A
   INNER JOIN QUERY_HISTORY B ON A.WAREHOUSE_NAME=B.WAREHOUSE_NAME
   AND A.START_TIME=B.START_TIME),
   
 -- Categorise and union actual snowflake warehouse cost, snowflake query execution cost and Idle cost.
 
OVERVIEW AS

  (SELECT START_TIME,
          WAREHOUSE_NAME,
          CRITERIA,
          ROUND(CREDITS, 2)CREDITS,
          ROUND(COST, 2) COST
   FROM WAREHOUSE_HISTORY
   WHERE CRITERIA='SNOWFLAKE_WAREHOUSE_COST'
   
   UNION 
   
   SELECT START_TIME,
                WAREHOUSE_NAME,
                CRITERIA,
                ROUND(CREDITS, 2)CREDITS,
                ROUND(COST, 2) COST
   FROM QUERY_HISTORY
   WHERE CRITERIA='SNOWFLAKE_WAREHOUSE_COST'
   
   UNION 
   
   SELECT START_TIME,
                WAREHOUSE_NAME,
                CRITERIA,
                ROUND(CREDITS, 2)CREDITS,
                ROUND(COST, 2) COST
   FROM IDLE_COST
   WHERE CRITERIA='SNOWFLAKE_WAREHOUSE_COST')
   
   
SELECT *
FROM OVERVIEW
WHERE START_TIME = :daterange AND WAREHOUSE_NAME=:Warehouse
ORDER BY START_TIME DESC
