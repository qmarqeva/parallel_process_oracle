-- Seguimiento de registros 
SELECT X.PROCESADA, 
COUNT(*) TOTAL
FROM USR_PARALLEL.TBL_A_PROCESAR X
GROUP BY X.PROCESADA; 

-- Monitoreo de Procesos
SELECT A.owner, A.job_name, A.STATE, A.comments
  FROM DBA_SCHEDULER_JOBS A
 WHERE A.job_name LIKE '%DUMMY_PARALLEL%'
