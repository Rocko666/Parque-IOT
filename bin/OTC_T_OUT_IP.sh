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
ENTIDAD=D_BIRPIOTM2MTTFAPN0020

#PARAMETROS DEFINIDOS EN LA TABLA params_des
VAL_FECHA_EJEC=$1

VAL_KINIT=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_LIB';"`
VAL_RUTA_IMP_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_RUTA_IMP_SPARK';"`
VAL_NOM_IMP_SPARK=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_NOM_IMP_SPARK';"`
VAL_NOM_JAR_ORC_11=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_SPARK_GENERICO' AND parametro = 'VAL_NOM_JAR_ORC_11';"`

#PARAMETROS DEFINIDOS EN LA TABLA params_des
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM2_VAL_COLA_EJECUCION';"`
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM3_VAL_CADENA_JDBC';"`
VAL_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM4_VAL_RUTA';"`
VAL_USUARIO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PARAM5_VAL_USUARIO';"`
TDUSER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDUSER';"`
TDPASS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPASS';"`
TDHOST=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDHOST';"`
TDPORT=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPORT';"`
TDDB=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDDB';"`
TDTABLE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDTABLE';"`
HIVEDB=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
VAL_MASTER=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_DIA=`date '+%Y%m%d'`
VAL_HORA=`date '+%H%M%S'`
VAL_LOG=$VAL_RUTA/log/OTC_T_OUT_IP_$VAL_DIA$VAL_HORA.log
VAL_JDBCURL=jdbc:oracle:thin:@$TDHOST:$TDPORT/$TDDB

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
	[ -z "$VAL_COLA_EJECUCION" ] || 
	[ -z "$VAL_CADENA_JDBC" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_USUARIO" ]  || 
	[ -z "$TDUSER" ] || 
	[ -z "$TDPASS" ] || 
	[ -z "$TDHOST" ] || 
	[ -z "$TDPORT" ] || 
	[ -z "$TDDB" ] || 
	[ -z "$TDTABLE" ] || 
	[ -z "$VAL_DIA" ] || 
	[ -z "$VAL_HORA" ] || 
	[ -z "$VAL_LOG" ]; then
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

#INICIO DEL PROCESO
echo "==== Inicia ejecucion del proceso extracion - Tabla OTC_T_OUT_IP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
echo "Fecha Ejecucion: $VAL_FECHA_EJEC" >> $VAL_LOG

#EXTRAE INFORMACION DE LA TABLA tmp_otc_t_ip
echo "==== Inicia ejecucion del import - Tabla tmp_otc_t_ip ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG

echo "Conexion a Oracle: $VAL_JDBCURL" >> $VAL_LOG
echo "Tabla Origen: R_OM_M2M_PI" >> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$TDTABLE" >> $VAL_LOG
echo "Proceso: $VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK " >> $VAL_LOG

#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--name TMP_OTC_T_IP \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--jars $VAL_RUTA_LIB/$VAL_NOM_JAR_ORC_11 \
$VAL_RUTA_IMP_SPARK/$VAL_NOM_IMP_SPARK \
--vclass=oracle.jdbc.driver.OracleDriver \
--vjdbcurl=$VAL_JDBCURL \
--vusuariobd=$TDUSER \
--vclavebd=$TDPASS \
--vhivebd=$HIVEDB \
--vtablahive=$TDTABLE \
--vtipocarga="overwrite" \
--vfilesql=$VAL_RUTA/sql/tmp_otc_t_ip.sql 2>&1 &>> $VAL_LOG

# VALIDA EJECUCION DEL ARCHIVO SPARK
# error_spark=`egrep 'Exception|Traceback|An error occurred|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
# if [ $error_spark -eq 0 ];then
# echo "==== OK - La ejecucion del archivo spark $VAL_NOM_IMP_SPARK  es EXITOSO ===="`date '+%H%M%S'` >> $VAL_LOG
# else
# echo "==== ERROR: - En la ejecucion del archivo $VAL_NOM_IMP_SPARK ====" >> $VAL_LOG
# exit 1
# fi

echo "==== Finaliza ejecucion del proceso extracion - Tabla OTC_T_OUT_IP ===="`date '+%Y%m%d%H%M%S'` >> $VAL_LOG
