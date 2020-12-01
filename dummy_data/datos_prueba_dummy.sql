-- Generar registros 'dummy' para pruebas

INSERT INTO USR_PARALLEL.TBL_A_PROCESAR
  (ID_REGISTRO)
  SELECT DBMS_RANDOM.string('U', 5) || '_' || (0 + LEVEL) AS IDENTIFICADOR
    FROM DUAL
  CONNECT BY LEVEL <= 500000;

COMMIT; 
