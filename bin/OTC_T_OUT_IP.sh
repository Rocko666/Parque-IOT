set -e
#########################################################################################################
# NOMBRE: OTC_T_OUT_IP.sh     			    	      								                     #
# DESCRIPCION:																						     #
#   Realiza el proceso de importacion de ORACLE a Hive, de la informacion de las ip		                 #
# AUTOR: Gustavo Uzcategui - Softconslting                  											 #
# FECHA CREACION: 2021-11-11   																		     #
# PARAMETROS DEL SHELL                                                    								 #
# VAL_FECHA_EJEC=${1} en formato  YYYYMMDD    		 												 	 #
#########################################################################################################
# MODIFICACIONES																					    #
# FECHA  		AUTOR     		DESCRIPCION MOTIVO											            #
# 2023-08-28	CRISTIAN ORTIZ		Migracion beeline->spark BIGD-216									#
#########################################################################################################
#########################################################################################################
##############
# VARIABLES #
##############
ENTIDAD=BIRPIOTM2MTTFAPN0020

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_FECHA_EJEC=$1

#PARAMETROS GENERICOS
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_LIB';"`
VAL_RUTA_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_IMP_SPARK';"`
VAL_NOM_IMP_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_IMP_SPARK';"`
VAL_NOM_JAR_ORC_11=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_NOM_JAR_ORC_11';"`
TDCLASS_ORC=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDCLASS_ORC';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
TDUSER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDUSER';"`
TDPASS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPASS';"`
TDHOST=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDHOST';"`
TDPORT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPORT';"`
TDDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDDB';"`
TDTABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDTABLE';"`
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TIPO_CARGA';"`
VAL_QUEUE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'QUEUE';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTOR_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTOR_CORES';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_DIA=`date '+%Y%m%d'`
VAL_HORA=`date '+%H%M%S'`
VAL_JDBCURL=jdbc:oracle:thin:@$TDHOST:$TDPORT/$TDDB
VAL_LOG=$VAL_RUTA/log/OTC_T_OUT_IP_$VAL_DIA$VAL_HORA.log

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
	[ -z "$VAL_FECHA_EJEC" ] || 
	[ -z "$VAL_RUTA_SPARK" ] || 
	[ -z "$VAL_RUTA_LIB" ] || 
	[ -z "$VAL_RUTA_IMP_SPARK" ]  || 
	[ -z "$VAL_NOM_IMP_SPARK" ] || 
	[ -z "$VAL_NOM_JAR_ORC_11" ] || 
	[ -z "$TDCLASS_ORC" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$TDUSER" ] || 
	[ -z "$TDPASS" ] || 
	[ -z "$TDHOST" ] || 
	[ -z "$TDPORT" ] || 
	[ -z "$TDDB" ] || 
	[ -z "$TDTABLE" ] || 
	[ -z "$HIVEDB" ] || 
	[ -z "$VAL_TIPO_CARGA" ] || 
	[ -z "$VAL_QUEUE" ] || 
	[ -z "$VAL_MASTER" ] || 
	[ -z "$VAL_DRIVER_MEMORY" ] || 
	[ -z "$VAL_EXECUTOR_MEMORY" ] || 
	[ -z "$VAL_NUM_EXECUTORS" ] || 
	[ -z "$VAL_NUM_EXECUTOR_CORES" ] || 
	[ -z "$VAL_JDBCURL" ] || 
	[ -z "$VAL_LOG" ]; then
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion del proceso extracion - Tabla OTC_T_OUT_IP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
echo "Fecha Ejecucion: $VAL_FECHA_EJEC" 2>&1 &>> $VAL_LOG

#EXTRAE INFORMACION DE LA TABLA tmp_otc_t_ip
echo "==== Inicia ejecucion del import - Tabla tmp_otc_t_ip ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG

echo "Conexion a Oracle: $VAL_JDBCURL" 2>&1 &>> $VAL_LOG
echo "Tabla Origen: R_OM_M2M_PI" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$TDTABLE" 2>&1 &>> $VAL_LOG
echo "Proceso: $VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK " 2>&1 &>> $VAL_LOG

#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--master $VAL_MASTER \
--name TMP_OTC_T_IP \
--queue $VAL_QUEUE \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTOR_CORES \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK \
--vclass=$TDCLASS_ORC \
--vjdbcurl=$VAL_JDBCURL \
--vusuariobd=$TDUSER \
--vclavebd=$TDPASS \
--vhivebd=$HIVEDB \
--vtablahive=$TDTABLE \
--vtipocarga=$VAL_TIPO_CARGA \
--vfilesql=$VAL_RUTA/sql/tmp_otc_t_ip.sql 2>&1 &>> $VAL_LOG

echo "==== Finaliza ejecucion del proceso extracion - Tabla OTC_T_OUT_IP ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
