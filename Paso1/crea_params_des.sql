--PARAMETROS PARA LA ENTIDAD D_BIRPIOTM2MTTFAPN0010
DELETE FROM params_des WHERE entidad='D_BIRPIOTM2MTTFAPN0010';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','SHELL','/home/nae108834/RGenerator/reportes/IOT_M2M_TRAFICO_APN/bin/OTC_T_IOT_M2M_TRAFICO_APN.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_RUTA','/home/nae108834/RGenerator/reportes/IOT_M2M_TRAFICO_APN','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','PARAM1_FECHA_EJEC','date_format(sysdate(),''%Y%m%d'')','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','ETAPA','1','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_DIAS_ATRAS','5','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','QUEUE','reportes','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_ESQUEMA_TMP','db_desarrollo2021','0','0'); -- db_temporales
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_ESQUEMA_REP','db_desarrollo2021','0','0');  --db_reportes
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_TABLA_FINAL','otc_t_iot_m2m_trafico_apn','0','0'); 
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_MASTER','yarn','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_DRIVER_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_EXECUTOR_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_NUM_EXECUTORS','6','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0010','VAL_NUM_EXECUTOR_CORES','6','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_BIRPIOTM2MTTFAPN0010';

--PARAMETROS PARA LA ENTIDAD D_BIRPIOTM2MTTFAPN0020
DELETE FROM params_des WHERE entidad='D_BIRPIOTM2MTTFAPN0020';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','SHELL','/home/nae108834/RGenerator/reportes/IOT_M2M_TRAFICO_APN/bin/OTC_T_OUT_IP.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','VAL_RUTA','/home/nae108834/RGenerator/reportes/IOT_M2M_TRAFICO_APN','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','PARAM1_FECHA_EJEC','date_format(sysdate(),''%Y%m%d'')','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','ETAPA','1','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','QUEUE','reportes','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','TDUSER','rdb_reportes','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','TDPASS','TelfEcu2017','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','TDHOST','proxfulldg1.otecel.com.ec','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','TDPORT','7594','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','TDDB','tomstby.otecel.com.ec','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','TDTABLE','tmp_otc_t_ip','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','HIVEDB','db_desarrollo2021','0','0'); --db_temporales
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','TIPO_CARGA','overwrite','0','0'); 
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','VAL_MASTER','local','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','VAL_DRIVER_MEMORY','8G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','VAL_EXECUTOR_MEMORY','8G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','VAL_NUM_EXECUTORS','4','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_BIRPIOTM2MTTFAPN0020','VAL_NUM_EXECUTOR_CORES','4','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_BIRPIOTM2MTTFAPN0020';
