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
ENTIDAD=BIRPIOTM2MTTFAPN0010
VAL_FECHA_EJEC=$1

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_DIAS_ATRAS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DIAS_ATRAS';"`
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"`
VAL_ESQUEMA_REP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_REP';"`
VAL_TABLA_FINAL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_FINAL';"`
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
VAL_QUEUE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'QUEUE';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTOR_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTOR_CORES';"`

#PARAMETROS GENERICOS
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FECHA_PROC=$(date -d "$(date -d $VAL_FECHA_EJEC +%Y%m%d) -1 days" +%Y%m%d)
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/log/OTC_T_IOT_M2M_TRAFICO_APN_$VAL_DIA$VAL_HORA.log

if  [ -z "$ENTIDAD" ] || 
	[ -z "$VAL_FECHA_EJEC" ] || 
	[ -z "$VAL_DIAS_ATRAS" ]|| 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_ESQUEMA_TMP" ] || 
	[ -z "$VAL_ESQUEMA_REP" ] || 
	[ -z "$VAL_TABLA_FINAL" ]|| 
	[ -z "$VAL_RUTA_SPARK" ] || 
	[ -z "$VAL_FECHA_PROC" ] || 
	[ -z "$ETAPA" ] || 
	[ -z "$VAL_QUEUE" ] || 
	[ -z "$VAL_MASTER" ] || 
	[ -z "$VAL_DRIVER_MEMORY" ] || 
	[ -z "$VAL_EXECUTOR_MEMORY" ] || 
	[ -z "$VAL_NUM_EXECUTORS" ] || 
	[ -z "$VAL_NUM_EXECUTOR_CORES" ] || 
	[ -z "$VAL_LOG" ]; then
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
if [ "$ETAPA" = "1" ]; then
#ETAPA 1: HACE EL LLAMADO DEL SHELL QUE CONTIENE LAS IP
echo "==== Inicia ejecucion del proceso de extraccion IOT M2M del trafico APN  ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "Fecha Ejecucion: $VAL_FECHA_EJEC" 2>&1 &>> $VAL_LOG
echo "==== Ejecuta SHELL SQOOP que genera las ip ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
sh -x $VAL_RUTA/bin/OTC_T_OUT_IP.sh $VAL_FECHA_EJEC

echo "==== Valida ejecucion del import ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
if [ $? -ne 0 ]; then
	echo "==== ERROR al ejecutar import ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
	exit 1
	else
	echo "==== OK - Ejecucion del import es EXITOSO ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
fi

ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
echo "==== OK - Se procesa la ETAPA 1 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
fi

if [ "$ETAPA" = "2" ]; then
#ETAPA 2: HACE EL LLAMADO AL SPARK QUE EXTRAE El PARQUE IOT M2M DEL TRAFICO APN
i=1
while [ $i -le ${VAL_DIAS_ATRAS} ]
do
VAL_FECHA_CARGA=$(date -d "$VAL_FECHA_EJEC -$i days" +"%Y%m%d");
echo "==== Ejecuta SPARK que carga_otc_t_iot_m2m_trafico_apn.py ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--name $ENTIDAD \
--master $VAL_MASTER \
--queue $VAL_QUEUE \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTOR_CORES \
$VAL_RUTA/python/carga_otc_t_iot_m2m_trafico_apn.py \
--vfecha_proceso=$VAL_FECHA_CARGA \
--vSchTmp=$VAL_ESQUEMA_TMP \
--vSchRep=$VAL_ESQUEMA_REP \
--vTFinal=$VAL_TABLA_FINAL 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL PYTHON
echo "==== Valida ejecucion del python ====" 2>&1 &>> $VAL_LOG
error_py=`egrep 'AnalysisException|TypeError:|FAILED:|Error|Table not found|Table already exists|Vertex|No such file or directory' $VAL_LOG | wc -l`
if [ $error_py -eq 0 ];then
	echo "==== OK - La ejecucion del python es EXITOSO ====" 2>&1 &>> $VAL_LOG
else
	echo "==== ERROR - En la ejecucion del python  ====" 2>&1 &>> $VAL_LOG
	exit 1
fi		

((i++))
done

#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo "==== OK - Se procesa la ETAPA 2 con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA' ;"`
fi
	
echo "==== Finaliza ejecucion del proceso OTC_T_IOT_M2M_TRAFICO_APN ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
