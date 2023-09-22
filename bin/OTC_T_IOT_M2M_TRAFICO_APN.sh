set -e
#########################################################################################################
# NOMBRE: OTC_T_IOT_M2M_TRAFICO_APN.sh     	     											         	#
# DESCRIPCION:																						    #
#  HQL que ejecuta el proceso ETL para tomar la informacion del parque IOT M2M del trafico APN y su IP  #
#  para cargar en la tabla destino otc_t_iot_m2m_trafico_apn en Hive particionada por fecha de proceso  #
#  en formato YYYYMMDD                                                                                  # 
# AUTOR: Gustavo Uzcategui - Softconsulting                            						            #
# FECHA CREACION: 2021-11-15   																	        #
# PARAMETROS DEL SHELL                            												        #
# VAL_FECHA_EJEC=${1} en formato  YYYYMMDD                                                              #
# VAL_COLA_EJECUCION={2} Nombre de la cola de ejecucion                                                 #
# VAL_CADENA_JDBC={3} Nombre de la cadena JDBC                                                          #
# VAL_RUTA={4}  Nombre de la ruta de la SHELL                                                           #
# VAL_USUARIO={5} Nombre del usuario que ejecuta la SHELL                                               #
#########################################################################################################
# MODIFICACIONES																						#
# FECHA  		AUTOR     			DESCRIPCION MOTIVO												    #
# 2023-08-28	CRISTIAN ORTIZ		Migracion beeline->spark BIGD-216									#
#########################################################################################################

##############
# VARIABLES #
##############
ENTIDAD=D_BIRPIOTM2MTTFAPN0010

#PARAMETROS DEFINIDOS EN LA TABLA params_des
VAL_FECHA_EJEC=$1
VAL_COLA_EJECUCION='reportes'
VAL_CADENA_JDBC='jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2?tez.queue.name=default'
VAL_RUTA='/home/nae108834/RGenerator/reportes/IOT_M2M_TRAFICO_APN'
VAL_USUARIO='rgenerator'

VAL_DIAS_ATRAS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DIAS_ATRAS';"`

VAL_KINIT=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FECHA_PROC=$(date -d "$(date -d $VAL_FECHA_EJEC +%Y%m%d) -1 days" +%Y%m%d)
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/log/OTC_T_IOT_M2M_TRAFICO_APN_$VAL_DIA$VAL_HORA.log


if  [ -z "$ENTIDAD" ] || 
	[ -z "$VAL_FECHA_EJEC" ] || 
	[ -z "$VAL_COLA_EJECUCION" ]|| 
	[ -z "$VAL_CADENA_JDBC" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_USUARIO" ] || 
	[ -z "$VAL_LOG" ]|| 
	[ -z "$VAL_FECHA_PROC" ] || 
	[ -z "$VAL_DIA" ] || 
	[ -z "$VAL_HORA" ]; then
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion del proceso de extraccion IOT M2M del trafico APN  ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Fecha Ejecucion: $VAL_FECHA_EJEC" >> $VAL_LOG

#PASO 1: HACE EL LLAMADO DEL SHELL QUE CONTIENE LAS IP
echo "==== Ejecuta SHELL SQOOP que genera las ip ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
sh -x $VAL_RUTA/bin/OTC_T_OUT_IP.sh $VAL_FECHA_EJEC

echo "==== Valida ejecucion del import ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
if [ $? -ne 0 ]; then
	echo "==== ERROR al ejecutar import ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
	exit 1
	else
	echo "==== OK - Ejecucion del import es EXITOSO ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
fi

#PASO 2: HACE EL LLAMADO AL HQL QUE EXTRAE El PARQUE IOT M2M DEL TRAFICO APN
i=1
while [ $i -le ${VAL_DIAS_ATRAS} ]
do
VAL_FECHA_CARGA=$(date -d "$VAL_FECHA_EJEC -$i days" +"%Y%m%d");
echo "==== Ejecuta HQL que carga_otc_t_iot_m2m_trafico_apn.sql ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
--hivevar pt_fecha=$VAL_FECHA_CARGA --hivevar fecha_proceso=$VAL_FECHA_CARGA \
-f ${VAL_RUTA}/sql/carga_otc_t_iot_m2m_trafico_apn.sql 2>> $VAL_LOG
 ((i++))
done

#VALIDA EJECUCION DEL HQL QUE EXTRAE El PARQUE IOT M2M DEL TRAFICO APN
echo "==== Valida ejecucion del HQL carga_otc_t_iot_m2m_trafico_apn.sql ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
if [ $? -eq 0 ]; then 
echo "El comando se ejecuto correctamente el comando HQL" 
echo "==== OK - La ejecucion del HQL carga_otc_t_iot_m2m_trafico_apn.sql es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
else 
echo "Hubo un error al ejecutar el comando HQL" 
echo "==== ERROR - En la ejecucion del HQL carga_otc_t_iot_m2m_trafico_apn.sql ====" >> $VAL_LOG
exit 1;
fi

echo "==== Finaliza ejecucion del proceso extraccion IOT M2M del trafico APN ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG