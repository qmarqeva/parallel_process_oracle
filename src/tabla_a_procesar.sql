-- Tabla para simular registros a procesar 

create table USR_PARALLEL.TBL_A_PROCESAR
(
  id_registro   VARCHAR2(25),
  procesada     VARCHAR2(2) default 'N',
  fecha_proceso DATE
);

create index USR_PARALLEL.IDX_001 on USR_PARALLEL.TBL_A_PROCESAR (id_registro);

comment on column USR_PARALLEL.TBL_A_PROCESAR.procesada
  is 'S | N';


