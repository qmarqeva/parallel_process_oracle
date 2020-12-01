create or replace package USR_PARALLEL.PKG_PARALLEL_PROCESS is

  -- Author  : qmarqeva
  -- Created : 2020/12/01
  -- Purpose : Caso de ejemplo para procesamiento paralelo

  --Tipos
  TYPE rango_type_rec is RECORD(
    id_rango NUMBER,
    inicio   USR_PARALLEL.TBL_A_PROCESAR.ID_REGISTRO%TYPE,
    fin      USR_PARALLEL.TBL_A_PROCESAR.ID_REGISTRO%TYPE);

  TYPE rangos_type IS TABLE OF rango_type_rec;

  --Variables
  TOTAL_PROCESOS_JOB NUMBER := 10;
  PREFIJO_JOB        VARCHAR2(25) := 'DUMMY_PARALLEL_';

  --Procedimientos
  PROCEDURE EJECUCION_PARALELA;

  PROCEDURE PROCESO_PARALELIZADO(P_REG_MINIMO IN USR_PARALLEL.TBL_A_PROCESAR.ID_REGISTRO%TYPE,
                                 P_REG_MAXIMO IN USR_PARALLEL.TBL_A_PROCESAR.ID_REGISTRO%TYPE);

  PROCEDURE ELIMINAR_TODOS;

end PKG_PARALLEL_PROCESS;
/
create or replace package body USR_PARALLEL.PKG_PARALLEL_PROCESS is

  /*
  * Número Total de Registros que serán procesados
  * y dividos en los procesos
  */
  FUNCTION GET_TOTAL_REGISTROS RETURN NUMBER IS
    V_TOTAL NUMBER;
  BEGIN
  
    SELECT COUNT(*)
      INTO V_TOTAL
      FROM USR_PARALLEL.TBL_A_PROCESAR S
     WHERE S.PROCESADA = 'N';
  
    RETURN V_TOTAL;
  
  END;

  /*
  * Generación de rangos que serán ejecutados por cada proceso 
  */
  FUNCTION GET_RANGOS_A_PROCESAR RETURN rangos_type IS
    TOTAL_REGISTROS       NUMBER;
    REGISTROS_POR_PROCESO NUMBER;
    REG_MINIMO            USR_PARALLEL.TBL_A_PROCESAR.ID_REGISTRO%TYPE;
    REG_MAXIMO            USR_PARALLEL.TBL_A_PROCESAR.ID_REGISTRO%TYPE;
    --
    v_desde number;
    v_hasta number;
    --    
    rangos     rangos_type := rangos_type();
    rango_indv rango_type_rec;
  BEGIN
  
    TOTAL_REGISTROS := GET_TOTAL_REGISTROS;
  
    IF (TOTAL_REGISTROS > 0) THEN
      --Determina el número de registros por cada proceso
      REGISTROS_POR_PROCESO := ceil(TOTAL_REGISTROS / TOTAL_PROCESOS_JOB);
      v_desde               := 1;
      v_hasta               := REGISTROS_POR_PROCESO;
    
      for i in 1 .. TOTAL_PROCESOS_JOB loop


        --Se ordenan y se extraen
        --El orden debe ser el mismo que se utiliza en el procesamiento
              
        BEGIN
          SELECT REGISTRO
            INTO REG_MINIMO
            FROM (SELECT PR_ORDEN.REGISTRO, ROWNUM ORDEN
                    FROM (SELECT S.ID_REGISTRO REGISTRO
                            FROM USR_PARALLEL.TBL_A_PROCESAR S
                           WHERE S.PROCESADA = 'N'
                           ORDER BY S.ID_REGISTRO) PR_ORDEN)
           WHERE ORDEN = V_DESDE;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            REG_MINIMO := NULL;
        END;
      
        IF (V_HASTA > TOTAL_REGISTROS) THEN
          V_HASTA := TOTAL_REGISTROS;
        END IF;
      
        BEGIN
          SELECT REGISTRO
            INTO REG_MAXIMO
            FROM (SELECT PR_ORDEN.REGISTRO, ROWNUM ORDEN
                    FROM (SELECT S.ID_REGISTRO REGISTRO
                            FROM USR_PARALLEL.TBL_A_PROCESAR S
                           WHERE S.PROCESADA = 'N'
                           ORDER BY S.ID_REGISTRO) PR_ORDEN)
           WHERE ORDEN = V_HASTA;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            REG_MAXIMO := NULL;
        END;
      
        IF (REG_MINIMO IS NULL OR REG_MAXIMO IS NULL) THEN
          CONTINUE;
        END IF;
      
        --se crea registro individual
        rango_indv.id_rango := i;
        rango_indv.inicio   := REG_MINIMO;
        rango_indv.fin      := REG_MAXIMO;
      
        --se agrega a la colección
        rangos.extend();
        rangos(i) := rango_indv;
      
        --Se calcula el nuevo rango
        v_desde := v_hasta + 1;
        v_hasta := v_desde + REGISTROS_POR_PROCESO;
      end loop;
    END IF;
    RETURN rangos;
  END;

  /*
  * Simulación de Proceso ejecutado por rango 
  */
  PROCEDURE PROCESO_PARALELIZADO(P_REG_MINIMO IN USR_PARALLEL.TBL_A_PROCESAR.ID_REGISTRO%TYPE,
                                 P_REG_MAXIMO IN USR_PARALLEL.TBL_A_PROCESAR.ID_REGISTRO%TYPE) IS
  
    cursor REGISTROS_A_PROCESAR is
      select S.ID_REGISTRO, S.PROCESADA, S.FECHA_PROCESO
        from USR_PARALLEL.TBL_A_PROCESAR S
       where S.PROCESADA = 'N'
            --rango 
         AND S.ID_REGISTRO between P_REG_MINIMO AND P_REG_MAXIMO;
  
  BEGIN
  
    FOR reg in REGISTROS_A_PROCESAR loop
    
      --Hacemos alguna operación costosa
      -- * Extracción de información de multiple tablas
      -- * Cálculos de valores
      -- * Llamado de servicios
      -- etc....
    
      --Simulamos haciendo una espera por registro
      dbms_lock.sleep(seconds => (1 / 1000));
    
      --Registramos termino de procesamiento
      UPDATE USR_PARALLEL.TBL_A_PROCESAR S
         SET S.PROCESADA = 'S', S.FECHA_PROCESO = sysdate
       WHERE S.ID_REGISTRO = REG.ID_REGISTRO;
    
      commit; --por registro
    
    END LOOP;
  
  END;

  /*
  * Validar si los procesos aún siguen ejecutándose
  * y esperar hasta que terminen
  */
  PROCEDURE MONITOREO_JOBS IS
    JOBS_PENDIENTES               NUMBER;
    INTERVALO_VERIFICACION_SEGNDS NUMBER := 2;
  BEGIN
    LOOP
    
      sys.DBMS_LOCK.sleep(INTERVALO_VERIFICACION_SEGNDS);
    
      SELECT COUNT(*)
        INTO JOBS_PENDIENTES
        FROM DBA_SCHEDULER_JOBS A
       WHERE A.job_name LIKE PREFIJO_JOB || '%';
    
      EXIT WHEN JOBS_PENDIENTES = 0;
    END LOOP;
  END;

  /*
  * Proceso Principal para iniciar el proceso Paralelo
  */
  PROCEDURE EJECUCION_PARALELA IS
    SENTENCIA_SQL VARCHAR2(255);
    --
    rangos rangos_type := rangos_type();
  BEGIN
  
    RANGOS := GET_RANGOS_A_PROCESAR;
  
    IF (RANGOS.COUNT() > 0) THEN
    
      for i in RANGOS.FIRST .. RANGOS.LAST loop
      
        SENTENCIA_SQL := 'BEGIN USR_PARALLEL.PKG_PARALLEL_PROCESS.PROCESO_PARALELIZADO(''' || RANGOS(i).INICIO ||
                         ''', ''' || --
                         RANGOS(i).FIN || --
                         '''); END; ';
      
        dbms_scheduler.create_job(job_name   => PREFIJO_JOB || RANGOS(i).ID_RANGO,
                                  job_type   => 'PLSQL_BLOCK',
                                  job_action => SENTENCIA_SQL,
                                  enabled    => TRUE,
                                  auto_drop  => TRUE, --hará que el job desaparezca al ejecutarse
                                  comments   => '[' || RANGOS(i).INICIO ||
                                                '] to [' || RANGOS(i).FIN || ']');
      
        COMMIT;
      
      end loop;
    
      MONITOREO_JOBS;
    
    END IF;
  
    dbms_output.put_line('Proceso terminado.');
  
  END;

  /*
  * Si existen procesos pendientes o con error podemos eliminarlos
  */
  PROCEDURE ELIMINAR_TODOS IS
    CURSOR C_JOBS IS
      SELECT A.OWNER || '.' || A.job_name JOB
        FROM DBA_SCHEDULER_JOBS A
       WHERE A.job_name LIKE PREFIJO_JOB || '%';
  
  BEGIN
    FOR REG IN C_JOBS LOOP
      --se detiene
      dbms_scheduler.stop_job(job_name => REG.JOB, force => TRUE);
      --se elimina
      dbms_scheduler.drop_job(job_name => REG.JOB, force => TRUE);
    END LOOP;
  END;

end PKG_PARALLEL_PROCESS;
/
