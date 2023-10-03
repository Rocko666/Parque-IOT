# -- coding: utf-8 --
import sys
reload(sys)
from query import *
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import subprocess
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

## STEP 1: Definir variables o constantes
vLogInfo='INFO:'
vLogError='ERROR:'

timestart = datetime.now()
## STEP 2: Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser.add_argument('--ventidad', required=False, type=str,help='Parametro de la entidad')
parser.add_argument('--vhivebd', required=True, type=str, help='Nombre de la base de datos hive (tabla de salida)')
parser.add_argument('--vfecha_fin', required=True, type=str,help='Parametro 1 de la query sql')
parser.add_argument('--vfecha_inicio', required=True, type=str,help='Parametro 2 de la query sql')
parser.add_argument('--vfecha_antes_ayer', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vdia_uno_mes_sig_frmt', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vultimo_dia_act_frmt', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vanio_mes', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vsolo_anio', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vsolo_mes', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vfecha_meses_atras', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vfecha_meses_atras1', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vfecha_meses_atras2', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vdia_uno_mes_act_frmt', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vdia_uno_mes_ant_frmt', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vval_usuario4', required=True, type=str,help='Parametro 3 de la query sql')
parser.add_argument('--vSchTmp', required=True, type=str,help='Parametro esquema temporal hive')
parser.add_argument('--vSchRep', required=True, type=str,help='Parametro reportes hive')

parametros = parser.parse_args()
vEntidad=parametros.ventidad
vBaseHive=parametros.vhivebd
vfecha_fin=parametros.vfecha_fin
vfecha_inicio=parametros.vfecha_inicio
vfecha_antes_ayer=parametros.vfecha_antes_ayer
vdia_uno_mes_sig_frmt=parametros.vdia_uno_mes_sig_frmt
vultimo_dia_act_frmt=parametros.vultimo_dia_act_frmt
vanio_mes=parametros.vanio_mes
vsolo_anio=parametros.vsolo_anio
vsolo_mes=parametros.vsolo_mes
vfecha_meses_atras=parametros.vfecha_meses_atras
vfecha_meses_atras1=parametros.vfecha_meses_atras1
vfecha_meses_atras2=parametros.vfecha_meses_atras2
vdia_uno_mes_act_frmt=parametros.vdia_uno_mes_act_frmt
vdia_uno_mes_ant_frmt=parametros.vdia_uno_mes_ant_frmt
vval_usuario4=parametros.vval_usuario4
vSchTmp=parametros.vSchTmp
vSchRep=parametros.vSchRep

## STEP 3: Inicio el SparkSession
spark = SparkSession. \
    builder. \
    config("hive.exec.dynamic.partition.mode", "nonstrict"). \
    enableHiveSupport(). \
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
app_id = spark._sc.applicationId
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))

##STEP 4:QUERYS
print(lne_dvs())
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
timestart_b = datetime.now()
try:
    print(lne_dvs())
    vStp01="Paso 1"
    print(lne_dvs())
    print(etq_info("Paso [1]: Ejecucion de funcion [tmp_otc_t_ip_max]- CREA LA TABLA TEMPORAL DE IP DE FECHA MAS RECIENTES"))
    print(lne_dvs())
    df_tmp_otc_t_ip_max=spark.sql(tmp_otc_t_ip_max()).cache()
    df_tmp_otc_t_ip_max.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_ip_max.createOrReplaceTempView("tmp_otc_t_ip_max")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_ip_max",str(df_tmp_otc_t_ip_max.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_ip_max",vle_duracion(ts_step_tbl,te_step_tbl))))
    

    vStp01="Paso 2"
    print(lne_dvs())
    print(etq_info("Paso [2]: Ejecucion de funcion [tmp_otc_t_ip_uni]- -CREA LA TABLA TEMPORAL DE IP CON REGISTROS UNICOS"))
    print(lne_dvs())
    df_tmp_otc_t_ip_uni=spark.sql(tmp_otc_t_ip_uni(vfecha_inicio,vfecha_fin)).cache()
    df_tmp_otc_t_ip_uni.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_ip_uni.createOrReplaceTempView("tmp_otc_t_ip_uni")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_ip_uni",str(df_tmp_otc_t_ip_uni.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_ip_uni",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 3"
    print(lne_dvs())
    print(etq_info("Paso [3]: Ejecucion de funcion [tmp_otc_t_iot_m2m]- CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M"))
    print(lne_dvs())
    df_tmp_otc_t_iot_m2m=spark.sql(tmp_otc_t_iot_m2m(vanio_mes)).cache()
    df_tmp_otc_t_iot_m2m.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_iot_m2m.createOrReplaceTempView("tmp_otc_t_iot_m2m")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_iot_m2m",str(df_tmp_otc_t_iot_m2m.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_iot_m2m",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 4"
    print(lne_dvs())
    print(etq_info("Paso [4]: Ejecucion de funcion [tmp_otc_t_iot_m2m_trafico_apn]-CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M"))
    print(lne_dvs())
    df_tmp_otc_t_iot_m2m_trafico_apn=spark.sql(tmp_otc_t_iot_m2m_trafico_apn(vfecha_meses_atras,vfecha_fin)).cache()
    df_tmp_otc_t_iot_m2m_trafico_apn.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_iot_m2m_trafico_apn.createOrReplaceTempView("tmp_otc_t_iot_m2m_trafico_apn")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_iot_m2m_trafico_apn",str(df_tmp_otc_t_iot_m2m_trafico_apn.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_iot_m2m_trafico_apn",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    vStp01="Paso 5"
    print(lne_dvs())
    print(etq_info("Paso [5]: Ejecucion de funcion [tmp_otc_t_iot_m2m_trafico_apn_sin_dup]- CREA LA TABLA TEMPORAL DEL PARQUE IOT M2M SIN DUPLICADOS"))
    print(lne_dvs())
    df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup=spark.sql(tmp_otc_t_iot_m2m_trafico_apn_sin_dup()).cache()
    df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup.printSchema()
    ts_step_tbl = datetime.now()
    df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup.createOrReplaceTempView("tmp_otc_t_iot_m2m_trafico_apn_sin_dup")
    print(etq_info(msg_t_total_registros_obtenidos("df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup",str(df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_tmp_otc_t_iot_m2m_trafico_apn_sin_dup",vle_duracion(ts_step_tbl,te_step_tbl))))
    
    
    vStp01="Paso 6"
    print(lne_dvs())
    print(etq_info("Paso [6]: Ejecucion de funcion [otc_t_iot_m2m_trafico_apn] - INSERTA LOS REGISTROS EN LA TABLA OTC_T_IOT_M2M_TRAFICO_APN)"))
    print(lne_dvs())
    df_otc_t_iot_m2m_trafico_apn=spark.sql(otc_t_iot_m2m_trafico_apn(vTablaPrevia,vanio_mes)).cache()
    df_otc_t_iot_m2m_trafico_apn.printSchema()
    ts_step_tbl = datetime.now()
    columns = spark.table(vBaseHive+"."+vTablaDestino).columns
    cols = []
    for column in columns:
        cols.append(column)
    df_otc_t_iot_m2m_trafico_apn = df_otc_t_iot_m2m_trafico_apn.select(cols)
    
    print(etq_info("REALIZA EL BORRADO DE PARTICIONES CORRESPONDIENTES AL MES DE PROCESO (DESDE EL DIA 1 HASTA DIA CAIDO)"))
    query_truncate = "ALTER TABLE "+vBaseHive+"."+vTablaDestino+" DROP IF EXISTS PARTITION (p_fecha_factura = " + str(i)+ ") purge"
    print(query_truncate)
    spark.sql(query_truncate)
    
    query_final="INSERT INTO "+vBaseHive+"."+vTablaDestino+" partition (p_fecha_factura) "
    query_final=query_final+(otc_t_iot_m2m_trafico_apn(vTablaPrevia, vanio_mes))
    print(lne_dvs())
    print(query_final)
    hive_hwc.executeUpdate(query_final)
    #df_otc_t_iot_m2m_trafico_apn.write.mode("append").insertInto(vBaseHive+"."+vTablaDestino)
    print(etq_info("Insercion Ok de la tabla destino: "+str(vTablaDestino))) 
    print(etq_info(msg_t_total_registros_hive("df_otc_t_iot_m2m_trafico_apn",str(df_otc_t_iot_m2m_trafico_apn.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df_otc_t_iot_m2m_trafico_apn",vle_duracion(ts_step_tbl,te_step_tbl))))
    
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp01,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()
    del df_otc_t_iot_m2m_trafico_apn
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(msg_e_ejecucion(vStpFin,str(e)))

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())
