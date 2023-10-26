------------------------------------------------------------------------------------------------------------------
-- NOMBRE: carga_otc_t_iot_m2m_trafico_apn.sql
-- DESCRIPCION:
--   HQL que ejecuta la creacion de la tabla temporal tmp_otc_t_nc_ip
--   HQL que ejecuta el proceso ETL para tomar la informacion del parque IOT M2M del trafico APN y su IP
--   para cargar en la tabla destino otc_t_iot_m2m_trafico_apn en Hive particionada por fecha de proceso
--   en formato YYYYMMDD
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

--	N01
--CREA LA TABLA TEMPORAL DE IP DE FECHA MAS RECIENTES
select count(1) from  db_desarrollo2021.tmp_otc_t_ip_max; --db_temporales

--N02
--CREA LA TABLA TEMPORAL DE IP CON REGISTROS UNICOS
select count(1) from   db_desarrollo2021.tmp_otc_t_ip_uni; --db_temporales


--N03
--CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M
select count(1) from   db_desarrollo2021.tmp_otc_t_iot_m2m ; --db_temporales

--N04
--CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M
select count(1) from   db_desarrollo2021.tmp_otc_t_iot_m2m_trafico_apn;  --db_temporales


--N05
--CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M SIN DUPLICADOS
select count(1) from   db_desarrollo2021.tmp_otc_t_iot_m2m_trafico_apn_sin_dup;  --db_temporales
