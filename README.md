# parallel_process_oracle

Contiene un ejemplo funcional del procesamiento con subprocesos (JOBs) en una base Oracle. Considerar que el número máximo de procesos permitidos está dado por el parámetro en la base: 

>JOB_QUEUE_PROCESSES

## DETALLES
- Se utiliza solamente un tabla con registros y un "delay" para simular un procesamiento costoso. 
- Se especifica un número máximo de subprocesos (jobs)
- Se espera a que todos los procesos terminen para terminar la ejecución. 

## VALIDACION

El orden de ejecución de los scripts debe ser el siguiente: 

- permisos.sql

Para crear el usuario: USR_PARALLEL y dar los permisos necesarios. 

- tabla_a_procesar.sql

Crea la tabla que simulará la información a procesar. 

- datos_prueba_dummy.sql

Carga los datos de prueba para el ejercicio. 

- USR_PARALLALEL.PKG_PARALLEL_PROCESS.pck

Paquete que realiza el procesamiento. 

- ejecucion_proceso.sql

Ejecuta el proceso.

- monitoreo_procesos.sql

Revisión de datos que se están procesando y los procesos ejecutándose. 
