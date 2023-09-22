------------------------------------------------------------------------------------------------------------------
-- NOMBRE: carga_otc_t_ip.sql
-- DESCRIPCION:
--   HQL que ejecuta la creacion de la tabla temporal tmp_otc_t_nc_ip
-- AUTOR: Gustavo Uzcategui - Softconsulting
-- FECHA CREACION: 2021-11-15
------------------------------------------------------------------------------------------------------------------
-- MODIFICACIONES
-- FECHA         AUTOR                      DESCRIPCION MOTIVO
-- YYYY-MM-DD    NOMBRE Y APELLIDO          MOTIVO DEL CAMBIO
------------------------------------------------------------------------------------------------------------------
--SET VARIABLES
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;

--EJECUTA EL BORRADO DE LAS TABLAS TEMPORALES AL INICIO
DROP TABLE IF EXISTS db_desarrollo2021.tmp_otc_t_ip ;

--CREA TABLA TEMPORAL CON LA INFORMACION DEL CLIENTE CON SU TIPO DE DOCUMENTO
CREATE TABLE db_desarrollo2021.tmp_otc_t_ip (
  num_telefonico string comment 'Numero de telefono',
  ip_address varchar(40) comment 'Direccion IP', 
  iccid varchar(4000) comment 'Numero serial Sim',
  created_whem varchar(10) comment 'Fecha de proceso'
  )
  STORED as ORC 
  TBLPROPERTIES ('transactional'='false', 'orc.compress'='SNAPPY','external.table.purge'='true');

