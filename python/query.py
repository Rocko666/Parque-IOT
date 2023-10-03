## N01
def tmp_otc_t_ip_max(fecha_meses_atras2,fecha_fin):
    qry="""
    SELECT 
        num_telefonico AS num_telefonico,  
        MAX(created_whem) AS created_whem
    FROM db_desarrollo2021.tmp_otc_t_ip --db_temporales
    GROUP BY num_telefonico
    """.format(fecha_meses_atras2=fecha_meses_atras2,fecha_fin=fecha_fin)
    print(qry)
    return qry
## N02
def tmp_otc_t_ip_uni():
    qry="""
    SELECT 
        a.num_telefonico AS num_telefonico, 
        a.ip_address AS ip_address, 
        a.iccid AS iccid, 
        a.created_whem AS created_whem
    FROM db_desarrollo2021.tmp_otc_t_ip a --db_temporales
    INNER JOIN db_desarrollo2021.tmp_otc_t_ip_max b -db_temporales
        ON a.num_telefonico = b.num_telefonico 
        AND a.created_whem = b.created_whem  -
    GROUP BY 
        a.num_telefonico,
        a.ip_address,
        a.iccid,
        a.created_whem
    """
    print(qry)
    return qry
## N03
def tmp_otc_t_iot_m2m(fecha_inicio,fecha_fin):
    qry="""
    SELECT 
        iotm2m.num_telefonico as num_telefonico,
        iotm2m.fecha_alta AS fecha_alta,
        iotm2m.account_num AS account_num,
        iotm2m.identificacion_cliente AS identificacion_cliente,
        iotm2m.nombre_cliente AS nombre_cliente,
        iotm2m.codigo_plan AS codigo_plan,
        iotm2m.nombre_plan AS nombre_plan,
        iotm2m.segmento AS segmento,
        iotm2m.sub_segmento AS sub_segmento,
        iotm2m.linea_negocio_homologado AS linea_negocio_homologado,
        iotm2m.fecha_proceso AS fecha_proceso
    FROM db_reportes.otc_t_360_general iotm2m
    WHERE ((iotm2m.fecha_proceso = ${fecha_proceso} 
    AND iotm2m.es_parque='SI') or (fecha_movimiento_mes = ${fecha_proceso} and estado_abonado = 'BAA'))
    AND codigo_plan in
    (SELECT codigo_plan FROM db_reportes.otc_t_iot_plan_m2m)
    """.format(fecha_inicio=fecha_inicio,fecha_fin=fecha_fin)
    print(qry)
    return qry
##N04
def tmp_otc_t_iot_m2m_trafico_apn(vanio_mes):
    qry="""
    SELECT 
        iotm2m.num_telefonico AS num_telefonico,
        substr(iotm2m.fecha_alta,1,10) AS fecha_alta,
        iotm2m.account_num AS account_num,
        iotm2m.identificacion_cliente AS identificacion_cliente,
        iotm2m.nombre_cliente AS nombre_cliente,
        iotm2m.codigo_plan AS codigo_plan,
        iotm2m.nombre_plan AS nombre_plan,
        iotm2m.segmento AS segmento,
        iotm2m.sub_segmento AS sub_segmento,
        iotm2m.linea_negocio_homologado AS linea_negocio_homologado,
        tfapn.numeroorigen AS numeroorigen,
        tfapn.apn AS apn,
        case when tecnologiaconexion=6 then sum(tfapn.total_datos/1048576) end mb_4g,
        case when tecnologiaconexion=1 then sum(tfapn.total_datos/1048576) end mb_3g,
        case when tecnologiaconexion=2 then sum(tfapn.total_datos/1048576) end mb_2g,
        case when tecnologiaconexion not in (1,2,6) then sum(tfapn.total_datos/1048576) end mb_sin_tec,
        ip.ip_address AS ip_address,
        tfapn.imsi AS imsi,
        ip.iccid AS iccid,
        iotm2m.fecha_proceso AS fecha_proceso
    FROM db_desarrollo2021.tmp_otc_t_iot_m2m iotm2m --db_temporales
    LEFT JOIN db_cmd.otc_t_dm_cur_t1_v2 tfapn 
        ON (iotm2m.num_telefonico=tfapn.numeroorigen AND tfapn.activity_start_dt = ${fecha_proceso})
    LEFT JOIN db_desarrollo2021.tmp_otc_t_ip_uni ip  --db_temporales
        ON iotm2m.num_telefonico = ip.num_telefonico 
    GROUP BY 
        iotm2m.num_telefonico,
        substr(iotm2m.fecha_alta,1,10),
        iotm2m.account_num,
        iotm2m.identificacion_cliente,
        iotm2m.nombre_cliente,
        iotm2m.codigo_plan,
        iotm2m.nombre_plan,
        iotm2m.segmento,
        iotm2m.sub_segmento,
        iotm2m.linea_negocio_homologado,
        tfapn.numeroorigen,
        tfapn.apn,
        tfapn.tecnologiaconexion,
        ip.ip_address,
        tfapn.imsi,
        ip.iccid,
        iotm2m.fecha_proceso
    """.format(vanio_mes=vanio_mes)
    print(qry)
    return qry

## N05
def tmp_otc_t_iot_m2m_trafico_apn_sin_dup(fecha_meses_atras,fecha_fin):
    qry="""
    SELECT 
        num_telefonico AS num_telefonico,
        fecha_alta AS fecha_alta,
        account_num AS account_num,
        identificacion_cliente AS identificacion_cliente,
        nombre_cliente AS nombre_cliente,
        codigo_plan AS codigo_plan,
        nombre_plan AS nombre_plan,
        segmento AS segmento,
        sub_segmento AS sub_segmento,
        linea_negocio_homologado AS linea_negocio_homologado,
        numeroorigen AS numeroorigen,
        apn AS apn,
        SUM(mb_4g) AS mb_4g,
        SUM(mb_3g) AS mb_3g,
        SUM(mb_2g) AS mb_2g,
        SUM(mb_sin_tec) AS mb_sin_tec,    
        ip_address AS ip_address,
        imsi AS imsi,
        iccid AS iccid,
        fecha_proceso AS fecha_proceso 
    FROM db_desarrollo2021.tmp_otc_t_iot_m2m_trafico_apn  --db_temporales
    where fecha_proceso between ${fecha_proceso} and ${fecha_proceso}
    GROUP BY 
        num_telefonico,
        fecha_alta,
        account_num,
        identificacion_cliente,
        nombre_cliente,
        codigo_plan,
        nombre_plan,
        segmento,
        sub_segmento,
        linea_negocio_homologado,
        numeroorigen,
        apn,
        ip_address,
        imsi,
        iccid,
        fecha_proceso
    """.format(fecha_meses_atras=fecha_meses_atras,fecha_fin=fecha_fin)
    print(qry)
    return qry

## N06
def otc_t_iot_m2m_trafico_apn(tabla, anio_mes):
    qry="""
    SELECT
        num_telefonico,
        fecha_alta,
        account_num,
        identificacion_cliente,
        nombre_cliente,
        codigo_plan,
        nombre_plan,
        segmento,
        sub_segmento,
        linea_negocio_homologado,
        numeroorigen,
        apn,
        mb_4g, 
        mb_3g,
        mb_2g,
        mb_sin_tec,
        ip_address,
        imsi,
        iccid
    FROM db_desarrollo2021.tmp_otc_t_iot_m2m_trafico_apn_sin_dup
    """.format(tabla=tabla, anio_mes=anio_mes)
    #print(qry)
    return qry  
