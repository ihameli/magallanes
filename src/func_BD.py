# -*- coding: utf-8 -*-

"""
MAGALLANES
----------

Large scale networks explorer tool using PlanetLab nodes


Software license: Academic Free License (AFL) [see afl-3.0.txt]

Manifest: [see manifest.sha1sum]

http://cnet.fi.uba.ar/en

Developers: Mauricio Anderson Ricci (b)
            J. Ignacio Alvarez-Hamelin (a,b)

             (a) CONICET, Argentina
             (b) Facultad de Ingeniería, Universidad de Buenos Aires
                 Argentina (http://www.fi.uba.ar)

"""

import csv
from os import system, listdir, remove
from os.path import exists, dirname, abspath
from sys import stdout
from psycopg2 import connect, Error as DatabaseError
from time import sleep, time, asctime, gmtime
from socket import gethostbyname
from subprocess import PIPE, Popen, call
from json import loads

directorio = dirname(abspath(__file__))
archivo_conf = directorio + '/files/program.conf'
archivo_direcciones = directorio + '/files/GeoLite2-Country-Blocks-IPv4.csv'
archivo_paises = directorio + '/files/GeoLite2-Country-Locations-en.csv'

# Read config file
config = {}
with open(archivo_conf) as f:
    for line in f:
        (key, value) = line.split(':')
        value = value.rstrip('\n')
        value = ''.join(e for e in value if e!=' ')
        key = ''.join(e for e in key if e!=' ')
        config[key] = value

###############################################################################

def conectar_BD():
    """ Generate DSN from config file """

    dbname = config['BD_name']
    user = config['BD_user']
    host = config['BD_host']
    password = config['BD_pass']

    DSN = 'dbname = %s user = %s host = %s password = %s' % (dbname, user, host, password)

    return DSN


def checkDB():
    """ Check connection to DB and if the tables exist. IF not then it create it. """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        # 'users': Register of users
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS users (usr varchar(50) primary key, pass varchar(50), slice_name varchar(50));""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        # 'analysis': Root table
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS analysis    (usr            varchar(50),
                                                                     slice_name      varchar(50),
                                                                     description     text,
                                                                     analysis_id     smallint primary key,
                                                                     num_nodes       int,
                                                                     nodes           text,
                                                                     nodes_stored    text,
                                                                     num_dest        int,
                                                                     common_dest     char,
                                                                     nodes_dest      text,
                                                                     TS              timestamp,
                                                                     TS_epoch        int,
                                                                     period          int,
                                                                     duration        int,
                                                                     pps             smallint,
                                                                     wait            smallint,
                                                                     gap_limit       smallint,
                                                                     max_loops       smallint,
                                                                     loop_action     smallint,
                                                                     trace_type      varchar(50),
                                                                     recalculation_TTL  char,
                                                                     recalculation_time int,
                                                                     ping_type       varchar(50),
                                                                     ping_sent       smallint,
                                                                     ping_ttl        smallint,
                                                                     stored          char,
                                                                     last_trace_id   bigint,
                                                                     last_ping_id    bigint,
                                                                     topology_state  char);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE analysis add constraint FK_to_user foreign key (usr) references users(usr)
                                   on update set null
                                   on delete set null;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""DELETE FROM analysis CASCADE WHERE stored IS NULL;""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_analysis ON analysis(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'nodes_working': Node in which there is a process running
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS nodes_working (node varchar(50), analysis_id smallint, work_type varchar(50));""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE nodes_working add constraint FK_to_analysis foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_nodes_working ON nodes_working(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'traceroutes': Result of traceroutes obtained from an exploration
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS traceroutes (analysis_id    smallint,
                                                                     trace_id        int,
                                                                     src             Inet,
                                                                     dst             Inet,
                                                                     sport           int,
                                                                     dport           int,
                                                                     hop_count       smallint,
                                                                     firsthop        smallint,
                                                                     stop_reason     varchar(30),
                                                                     stop_data       smallint,
                                                                     TS              timestamp,
                                                                     TS_epoch        int,
																	 path            text,
                                                                     primary key(analysis_id, trace_id));""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE traceroutes add constraint FK_to_analysis foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_traceroutes ON traceroutes(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_traceroutes_trace ON traceroutes(trace_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'hops': Result of hops obtained from an exploration
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS hops       (analysis_id    smallint,
                                                                    trace_id        int,
                                                                    hop_n           smallint,
                                                                    hop_ip          Inet,
                                                                    hop_ip_previous Inet,
                                                                    rtt             real,
                                                                    reply_ttl       smallint,
                                                                    probe_size      smallint,
                                                                    reply_size      smallint,
                                                                    reply_ipid      int,
                                                                    reply_tos       smallint,
                                                                    icmp_type       smallint,
                                                                    icmp_code       smallint,
                                                                    icmp_q_ttl      smallint,
                                                                    icmp_q_len      smallint,
                                                                    icmp_q_tos      smallint,
                                                                    MPLS_numlabel   smallint,
                                                                    MPLS_label      varchar(200),
                                                                    primary key(analysis_id, trace_id, HOP_n));""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE hops add constraint FK_to_traceroutes foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_hops ON hops(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_hops_trace ON hops(trace_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'ping': Result of ping obtained from an exploration
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS ping       (analysis_id    smallint,
                                                                    ping_id         int,
                                                                    src             Inet,
                                                                    dst             Inet,
                                                                    TS_epoch        int,
                                                                    ping_sent       smallint,
                                                                    replies         smallint,
                                                                    loss            smallint,
                                                                    reply_ttl       varchar(100),
                                                                    ttl_unique      char,
                                                                    primary key(analysis_id, ping_id));""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE ping add constraint FK_to_analysis_from_ping foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_ping ON ping(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'alias_resolution': Root table for alias resolution
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS alias_resolution (analysis_id      smallint,
                                                                           estado           char,
                                                                           nodos            text,
                                                                           mper_port        int,
                                                                           mper_pps         smallint,
                                                                           est_duration     smallint,
                                                                           est_rounds       smallint,
                                                                           elim_rounds      smallint,
                                                                           cor_rounds       smallint,
                                                                           est_overlap      smallint,
                                                                           disc_overlap     smallint,
                                                                           elim_overlap     smallint,
                                                                           cor_overlap      smallint,
                                                                           cor_concurrency  smallint,
                                                                           elim_concurrency smallint,
                                                                           TS               timestamp);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE alias_resolution add constraint FK_to_analysis_from_ar foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_alias_resolution ON alias_resolution(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'routers': Definition of routes related to a alias resolution process
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS routers          (analysis_id      smallint,
                                                                           router_id        int primary key,
                                                                           num_interfaces   smallint,
                                                                           continent        varchar(100),
                                                                           cod_continent    varchar(10),
                                                                           country          varchar(100),
                                                                           cod_country      varchar(10),
                                                                           region           varchar(100),
                                                                           cod_region       varchar(10),
                                                                           city             varchar(100),
                                                                           latitude         real,
                                                                           longitude        real);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE routers add constraint FK_to_AR_from_routes foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_routers ON routers(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'interfaces': Definition of interfaces of routers related to a alias resolution process
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS interfaces       (analysis_id      smallint,
                                                                           router_id        int,
                                                                           IP               Inet);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE interfaces add constraint FK_to_AR_interfaces foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_interfaces ON interfaces(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'ip_found': All IPs found in an exploration
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS ip_found  (analysis_id      smallint,
                                                                    ip_found         Inet);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE ip_found add constraint FK_to_AR_ip_found foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_ip_found ON ip_found(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'ip_resolucion': IPs which are being resolving in a remote node
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS ip_resolution    (analysis_id      smallint,
                                                                           node             varchar(50),
                                                                           IP               Inet);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE ip_resolution add constraint FK_to_AR_ip_resolution foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_ip_resolution ON ip_resolution(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'ip_metodo_midar': The prefered method found for each IP in the estimation stage of the MIDAR
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS ip_metodo_midar  (analysis_id      smallint,
                                                                           IP               Inet,
                                                                           metodo           varchar(10));""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE ip_metodo_midar add constraint FK_to_AR_ip_metodo foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_ip_metodo_midar ON ip_metodo_midar(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'links_IP': Links found at IP level
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS links_IP      (analysis_id  smallint,
                                                                           IP1          Inet,
                                                                           IP2          Inet)""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE links_IP add constraint FK_to_AR_links_IP foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_links_IP ON links_IP(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'links': Links found at Router level
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS links            (analysis_id  smallint,
                                                                           R1           int,
                                                                           R2           int)""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        try:
            cursor.execute("""ALTER TABLE links add constraint FK_to_AR_links foreign key (analysis_id) references analysis(analysis_id)
                                   on update cascade
                                   on delete cascade;""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_links ON links(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'planetlab_nodes'
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS planetlab_nodes  (node_id          int primary key,
                                                                           hostname         varchar(100),
                                                                           ip               Inet,
                                                                           boot_state       varchar(50),
                                                                           site_id          int,
                                                                           site_name        varchar(300),
                                                                           site_abbr        varchar(100),
                                                                           latitude         real,
                                                                           longitude        real);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        #try:
        #    cursor.execute("""CREATE INDEX Index_planetlab_nodes ON planetlab_nodes(analysis_id);""")
        #    conexion.commit()
        #except DatabaseError:
        #    conexion.rollback()

        # 'address_block_ipv4'
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS address_block_ipv4 (network                Inet,
                                                                             continent_name         varchar(50),
                                                                             continent_code         char(2),
                                                                             country_name           varchar(50),
                                                                             country_iso_code       char(2));""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        # 'blacklist': Nodes which will no be use
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS blacklist_nodes_exploration (node varchar(100) primary key);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()

        # 'nodes_priority_for_AR': Priority of nodes to choose nodes for alias resolution in base on its priority
        try:
            cursor.execute("""CREATE TABLE IF NOT EXISTS nodes_priority_for_AR (node varchar(100) UNIQUE, priority smallint);""")
            conexion.commit()
        except DatabaseError:
            conexion.rollback()
        #

        cursor.close()
        conexion.close()

        salida = True

    except DatabaseError as e:
        salida = False
        print 'Error: ', str(e)

    else:
        cargarDatosDirecciones('inicializar')

    return salida

def actualizarDatosNodos(api_server, auth, slice_name):
    """ Update information of PlanetLab nodes on BD """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        stdout.write('\r\tUpdating information of nodes: ')
        stdout.flush()

        cursor.execute("""TRUNCATE planetlab_nodes;""")

        nodos = api_server.GetNodes (auth, {}, ['node_id', 'hostname', 'boot_state', 'site_id'])
        sitios = api_server.GetSites (auth, {}, ['site_id', 'name', 'abbreviated_name', 'latitude', 'longitude'])

        datos = []
        for x in nodos:
            for y in sitios:
                if x.get('site_id') == y.get('site_id'):
                    try:
                        ip = str(gethostbyname(x.get('hostname')))
                    except:
                        ip = None
                    finally:
                        try:
                            if type(y.get('latitude')) == float:
                                datos.append((x.get('node_id'), x.get('hostname'), ip, x.get('boot_state'), x.get('site_id'), y.get('name'), y.get('abbreviated_name'), y.get('latitude'), y.get('longitude')))
                            else:
                                datos.append((x.get('node_id'), x.get('hostname'), ip, x.get('boot_state'), x.get('site_id'), y.get('name'), y.get('abbreviated_name'), None, None))
                        except:
                            if type(y.get('latitude')) == float:
                                datos.append(x.get('node_id'), x.get('hostname'), ip, x.get('boot_state'), x.get('site_id'), None, None, y.get('latitude'), y.get('longitude'))
                            else:
                                datos.append(x.get('node_id'), x.get('hostname'), ip, x.get('boot_state'), x.get('site_id'), None, None, None, None)
                        finally:
                            break

        query = """INSERT INTO planetlab_nodes (node_id, hostname, ip, boot_state, site_id, site_name, site_abbr, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.executemany(query, datos)
        conexion.commit()

        cursor.close()
        conexion.close()

        stdout.write('\r\tUpdating information of nodes: Done\n')
        stdout.flush()

        salida = True

    except DatabaseError:
        stdout.write('\r\tUpdating information of nodes: Error\n')
        stdout.flush()
        salida = False

    except KeyboardInterrupt:
        stdout.write('\r\tUpdating information of nodes: Interrupted\n')
        stdout.flush()
        salida = False

    return None

def vaciarTemp():
    """ Remove files in Temp folder """

    archivos = [x for x in listdir('%s/temp' % directorio)]

    for x in archivos:
        remove('%s/temp/%s' % (directorio, x))

    return None

def generar_ID_analisis(parametros):
    """ Generate new ID for a new exploration """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        cursor.execute("""SELECT MAX(analysis_id) FROM analysis""")

        try:
            parametros['analysis_id'] = str( int(cursor.fetchall()[0][0]) + 1 )
        except:
            parametros['analysis_id'] = '0'

        cursor.execute("""INSERT INTO analysis (analysis_id, usr) VALUES (%s, %s);""", ( parametros.get('analysis_id'), str(parametros.get('usuario')) ))

        conexion.commit()
        cursor.close()
        conexion.close()

    except DatabaseError as e:
        print 'ERROR: %s' % (str(e))
        conexion.rollback()

    return None


def registrarAnalisis(parametros):
    """ Generate a new register on ANALYSIS table """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        if parametros.get('mismos_destinos') == 'S':
            common_dest = 'y'
            destinos = '|'.join(parametros.get('nodos_destino')[0])
        else:
            common_dest = 'n'
            aux = []
            for x in parametros.get('nodos_destino'):
                  aux.append('|'.join(x))
            destinos = '||'.join(aux)

        if parametros.get('ping_ejecutar') == 'S':

            datos = (parametros.get('slice_name'), parametros.get('descripcion'), len(parametros.get('nodos_origen')), '|'.join(parametros.get('nodos_origen')),
                    len(parametros.get('nodos_destino')[0]), common_dest, destinos, asctime(gmtime(time())), int(time()), parametros.get('periodo_traceroutes'),
                    parametros.get('duracion_traceroutes'), parametros.get('pps'), parametros.get('wait'), parametros.get('trace_type'),
                    parametros.get('recalcular_TTL'), parametros.get('tiempo_recalculo_TTL'), 'n', '0', parametros.get('gaplimit'),
                    parametros.get('max_loops'), parametros.get('loop_action'), parametros.get('ping_type'), parametros.get('ping_sent'), parametros.get('ping_ttl'))

            cursor.execute("""UPDATE analysis SET (slice_name, description, num_nodes, nodes, num_dest, common_dest, nodes_dest, TS, TS_epoch,
                    period, duration, pps, wait, trace_type, recalculation_TTL, recalculation_time, stored, topology_state, gap_limit, max_loops,
                    loop_action, ping_type, ping_sent, ping_ttl) =  """ + str(datos) + ' where analysis_id=' + str(parametros.get('analysis_id')) + ';')

        else:
			if parametros.get('trace_type') != 'B2B':
				datos = (parametros.get('slice_name'), parametros.get('descripcion'), len(parametros.get('nodos_origen')), '|'.join(parametros.get('nodos_origen')),
						len(parametros.get('nodos_destino')[0]), common_dest, destinos, asctime(gmtime(time())), int(time()), parametros.get('periodo_traceroutes'),
						parametros.get('duracion_traceroutes'), parametros.get('pps'), parametros.get('wait'), parametros.get('trace_type'),
						parametros.get('recalcular_TTL'), parametros.get('tiempo_recalculo_TTL'), 'n', '0', parametros.get('gaplimit'),
						parametros.get('max_loops'), parametros.get('loop_action'))

				cursor.execute("""UPDATE analysis SET (slice_name, description, num_nodes, nodes, num_dest, common_dest, nodes_dest, TS, TS_epoch,
						period, duration, pps, wait, trace_type, recalculation_TTL, recalculation_time, stored, topology_state, gap_limit, max_loops,
						loop_action) =  """ + str(datos) + ' where analysis_id=' + str(parametros.get('analysis_id')) + ';')
			else:
				datos = (parametros.get('slice_name'), parametros.get('descripcion'), len(parametros.get('nodos_origen')), '|'.join(parametros.get('nodos_origen')),
						len(parametros.get('nodos_destino')[0]), common_dest, destinos, asctime(gmtime(time())), int(time()),
						parametros.get('duracion_traceroutes'), parametros.get('trace_type'), 'n', '0')

				cursor.execute("""UPDATE analysis SET (slice_name, description, num_nodes, nodes, num_dest, common_dest, nodes_dest, TS, TS_epoch,
						duration, trace_type, stored, topology_state) =  """ + str(datos) + ' where analysis_id=' + str(parametros.get('analysis_id')) + ';')


        datos = []
        [datos.append((x, parametros.get('analysis_id'))) for x in parametros.get('nodos_origen')]
        cursor.executemany("""INSERT INTO nodes_working (node, analysis_id) VALUES (%s, %s)""", datos)

        conexion.commit()
        cursor.close()
        conexion.close()

        print '\nExploration registered\n'

    except DatabaseError as e:
        print 'Error:\n %s' % str(e)
        conexion.rollback()

    return None

def cargarDatosDirecciones(accion):
    """ Write data of IPv4 blocks from database of MaxMind """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        if accion == 'inicializar':
            pass
        elif accion == 'actualizar':
            cursor.execute("""TRUNCATE address_block_ipv4;""")

        # Load data files with the information of addresses blocks
        cursor.execute("""SELECT * FROM address_block_ipv4 limit 1""")
        registros = cursor.fetchall()

        if not registros:
            datos = []
            try:
                archivo = open(archivo_paises, 'r')
                paises = list(csv.reader(archivo, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL))
                paises.pop(0)
                archivo.close()
                archivo = open(archivo_direcciones, 'r')
                direcciones = list(csv.reader(archivo, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL))
                direcciones.pop(0)
                archivo.close()
            except:
                print 'Warning: MaxMind database file not found'
            else:
                print 'Updating table << address_block_ipv4 >>\n'
                print  "\tTime: ", asctime(gmtime(time()))
                referencia = int(time())

                try:
                    for i in direcciones:
                        for j in paises:
                            if i[1]==j[0]:
                                datos.append((i[0], j[2], j[3], j[4], j[5]))
                                break

                    query = """INSERT INTO address_block_ipv4 (network, continent_code, continent_name, country_iso_code, country_name) VALUES (%s, %s, %s, %s, %s)"""

                    cursor.executemany(query, datos)
                    conexion.commit()
                    print "\nSUCCESS!"
                    print  "\n\tDuration: ", str((int(time()) - referencia)/60), "min\n"

                except DatabaseError as e:
                    print 'Error:\n %s' % str(e)
                    conexion.rollback()

        cursor.close()
        conexion.close()

    except DatabaseError:
        pass

    return None

def confirmar(texto):
    """ Request confirmation """

    while True:
        conf = raw_input('\n' + texto + '? (y/n):\t ')
        if conf.lower() == 'y' or conf.lower() == 'n':
            break
            print '\n'

    if conf.lower() == 'y':
        continuar = True
    else:
        continuar = False

    return continuar

def SWAP(sudoPassword):
    """ Swapping memory if is possible """

    try:
        a = open('/proc/meminfo','r')
        datos = a.readlines()
        a.close()
        memoria = {}
        for x in datos:
            x = x.split()
            memoria[x[0]] = x[1]

        stdout.write('\r\tSWAP to RAM..............'+str())
        stdout.flush()

        if int(memoria.get('MemFree:')) > (int(memoria.get('SwapTotal:')) - int(memoria.get('SwapFree:'))):
            p = system('echo %s | sudo -S %s   >/dev/null 2>&1' % (sudoPassword, 'swapoff -a'))
            sleep(1)
            p = system('echo %s | sudo -S %s   >/dev/null 2>&1' % (sudoPassword, 'swapon -a'))
            stdout.write('\r\tSWAP to RAM..............SUCCESS\n')
            stdout.flush()
        else:
            stdout.write('\r\tSWAP to RAM..............THERE IS NOT ENOUGH FREE MEMORY\n')
            stdout.flush()

    except:
        stdout.write('\r\tSWAP to RAM..............ERROR\n')
        stdout.flush()

    return None

def almacenar_mediciones_BD(auth, slice_name, ID, nodos_almacenar, sudoPassword = None):
    """ Store in DB the data of exploration in the nodes <nodos_almacenar> """

    IdFile = '_%s' % ID

    user = auth.get('Username')

    nro_nodo = 0
    total_nodos = len(nodos_almacenar)
    nodos_preparados = []

    conexion = connect(conectar_BD())
    cursor = conexion.cursor()

    cursor.execute('SELECT max(last_trace_id) FROM analysis WHERE analysis_id=%s;' % ID)
    try:
        aux = cursor.fetchall()[0][0]
        trace_id = int(aux)
    except:
        trace_id = 0

    cursor.execute('SELECT max(last_ping_id) FROM analysis WHERE analysis_id=%s;' % ID)
    try:
        aux = cursor.fetchall()[0][0]
        ping_id = int(aux)
    except:
        ping_id = 0

    try:
        for nodo in nodos_almacenar:
            try:
                nro_nodo += 1
                print '\n\tNode ' + str(nro_nodo) + '/' + str(total_nodos) + ': ', nodo + '\n'

                vaciarTemp()

                print 'Copying files of results\n'
                comando = 'scp '+slice_name+'@'+nodo+':/home/'+slice_name+'/'+user+'/resultados'+IdFile+'.warts.gz ' + directorio + '/temp/'
                call([comando], shell=True)

                if exists(directorio + '/temp/resultados'+IdFile+'.warts.gz'):
                    print '\nDecompressing files'
                    proceso = Popen(['gzip -d '+directorio+'/temp/resultados'+IdFile+'.warts.gz'], shell=True)
                    proceso.wait()

                    if exists(directorio + '/temp/resultados'+IdFile+'.warts'):
                        print '\nConverting format warts to json'
                        proceso = Popen(['cd ' + directorio + '/temp/ ; sc_warts2json resultados'+IdFile+'.warts > resultados'+IdFile+'.json'], shell=True)
                        proceso.wait()

                        SWAP(sudoPassword)

                        if exists(directorio + '/temp/resultados'+IdFile+'.json'):
                            lineas = sum(1 for line in open(directorio + '/temp/resultados'+IdFile+'.json','r'))-1

                            if lineas:

                                lineas_por_bloque = min(30 * 1000, lineas / 100)

                                # Number of registers to store by time
                                cant_bloque_enteros = lineas / lineas_por_bloque
                                cant_lineas_ultimo_bloque = lineas % lineas_por_bloque
                                cant_bloque_total = cant_bloque_enteros
                                if cant_lineas_ultimo_bloque > 0:
                                    cant_bloque_total += 1

                                # MPLS
                                proceso = Popen(['cd ' + directorio + '/temp/ ; sc_analysis_dump -CcslerHiM -S 1 resultados'+IdFile+'.warts > resultados'+IdFile+'.txt'], shell=True)
                                proceso.wait()

                                lineas_MPLS = sum(1 for line in open(directorio + '/temp/resultados'+IdFile+'.txt','r'))

                                cant_bloque_enteros_MPLS = lineas_MPLS / lineas_por_bloque
                                cant_lineas_ultimo_bloque_MPLS = lineas_MPLS % lineas_por_bloque
                                cant_bloque_total_MPLS = cant_bloque_enteros_MPLS
                                if cant_lineas_ultimo_bloque_MPLS > 0:
                                    cant_bloque_total_MPLS += 1

                                print '\nGetting MPLS tags'
                                MPLS_etiqueta = {}
                                ejecutar = True
                                nro_bloque = 0

                                while ejecutar:
                                    
                                    # Read traceroutes in the block
                                    nro_bloque += 1
                                    salida_MPLS = []
                                    if nro_bloque == cant_bloque_total_MPLS:
                                        ejecutar = False

                                    with open(directorio + '/temp/resultados'+IdFile+'.txt') as f:
                                        fin_lectura = lineas_por_bloque * nro_bloque
                                        inicio_lectura = lineas_por_bloque * (nro_bloque-1)
                                        for n, line in enumerate(f):
                                            if (inicio_lectura) < n <= (fin_lectura):
                                                salida_MPLS.append(line)
                                                if n == fin_lectura:
                                                    break

                                    # Analyze each traceroute
                                    for x in salida_MPLS:
                                        # Check if there is at least one MPSL tag
                                        if 'M' in x:
                                            # if there is a tag then split
                                            x = x.split()
                                            # if it already exist then add the IP tag
                                            if x[1] in MPLS_etiqueta:
                                                MPLS_etiqueta[x[1]][int(x[2])] = {}
                                                for hop in x[4:]:
                                                    if 'M' in hop:
                                                        hop = hop.split(',')
                                                        MPLS_etiqueta[x[1]][int(x[2])][hop[0]] = hop[1:]

                                            # If it not already a dictionary then it's created using the desteny like key
                                            else:
                                                MPLS_etiqueta[x[1]] = {}
                                                MPLS_etiqueta[x[1]][int(x[2])] = {}
                                                for hop in x[4:]:
                                                    if 'M' in hop:
                                                        hop = hop.split(',')
                                                        MPLS_etiqueta[x[1]][int(x[2])][hop[0]] = hop[1:]
                                ## MPLS

                                print '\nProcessing results (%s register to stored)' % lineas
                                print "\tTime: ", asctime(gmtime(time()))
                                referencia = int(time())
                                ejecutar = True
                                nro_bloque = 0
                                count_trace = 0
                                count_trace_error = 0

                                while ejecutar:

                                    # Percentage of progress
                                    pct = (nro_bloque*100.0)/cant_bloque_total
                                    stdout.write('\r\tComplete .............. %d/100' % pct)
                                    stdout.flush()

                                    # Read traceroutes in the block
                                    salida = []
                                    nro_bloque += 1
                                    if nro_bloque == cant_bloque_total:
                                        ejecutar = False

                                    with open(directorio + '/temp/resultados'+IdFile+'.json') as f:
                                        fin_lectura = lineas_por_bloque * nro_bloque
                                        inicio_lectura = lineas_por_bloque * (nro_bloque-1)
                                        for n, line in enumerate(f):
                                            if (inicio_lectura) < n <= (fin_lectura):
                                                salida.append(line)
                                                if n == fin_lectura:
                                                    break

                                    trace = set()
                                    hops = set()
                                    ping_datos = set()

                                    # Analyze each traceroute/ping in the block
                                    for x in salida:
                                        try:
                                            x = loads(x)
                                        except:
                                            pass
                                        else:
                                            if x.get('type') == 'trace':
                                                if x.get('stop_reason') != 'ERROR':

                                                    trace_id += 1
                                                    count_trace += 1

                                                    trace.add((ID, trace_id, x.get('src'), x.get('dst'), x.get('sport'), x.get('dport'), x.get('firsthop'),
                                                    x.get('hop_count'), x.get('stop_reason'), x.get('stop_data'), x.get('start').get('ftime'), x.get('start').get('sec')))

                                                    # Check if there is hops which previously responded
                                                    if x.get('hops'):
                                                        # Analyze all hops which responded
                                                        control_attemps = set()
                                                        hop_n_anterior = 0
                                                        MPLS_ip = {}
                                                        if x.get('dst') in MPLS_etiqueta:
                                                            if x.get('start').get('sec') in MPLS_etiqueta.get(x.get('dst')):
                                                                MPLS_ip = MPLS_etiqueta.get(x.get('dst')).get(x.get('start').get('sec'))

                                                        for y in x.get('hops'):
                                                            if y.get('probe_ttl') not in control_attemps: # Control that it is saved only one attempt

                                                                control_attemps.add(y.get('probe_ttl'))

                                                                MPLS = MPLS_ip.get(y.get('addr'))

                                                                if MPLS:
                                                                    MPLS_label = ';'.join(MPLS)
                                                                    MPLS_numlabel = len(MPLS)
                                                                else:
                                                                    MPLS_label = None
                                                                    MPLS_numlabel = 0

                                                                # Get the previous hop to find the IP neighbours
                                                                if y.get('probe_ttl') == 1:
                                                                    HOP_IP_previous = x.get('src')
                                                                elif y.get('probe_ttl') == hop_n_anterior + 1:
                                                                    HOP_IP_previous = hop_ip_anterior
                                                                else:
                                                                    HOP_IP_previous = None

                                                                hops.add((ID, trace_id, y.get('probe_ttl'), y.get('addr'), HOP_IP_previous, y.get('rtt'),
                                                                y.get('reply_ttl'), y.get('probe_size'), y.get('reply-size'), y.get('reply_ipid'), y.get('reply_tos'),
                                                                y.get('icmp_type'), y.get('icmp_code'), y.get('icmp_q_ttl'), y.get('icmp_q_ipl'), y.get('icmp_q_tos'), MPLS_label, MPLS_numlabel))

                                                                hop_n_anterior = y.get('probe_ttl')
                                                                hop_ip_anterior = y.get('addr')

                                                else:
                                                    count_trace_error += 1

                                            elif x.get('type') == 'ping':
                                                ping_id += 1
                                                reply_ttl = []

                                                if x.get('responses'):
                                                    aux = x.get('responses')[0].get('reply_ttl')
                                                    [reply_ttl.append(str(ping.get('reply_ttl'))) for ping in x.get('responses')]
                                                    if len(set(reply_ttl))==1:
                                                        ttl_unique = 'y'
                                                    else:
                                                        ttl_unique = 'n'
                                                else:
                                                    ttl_unique = None

                                                ping_datos.add((ID, ping_id, x.get('src'), x.get('dst'), x.get('start').get('sec'), x.get('ping_sent'), x.get('statistics').get('replies'), x.get('statistics').get('loss'), '|'.join(reply_ttl), ttl_unique))

                                            else:
                                                print 'TypeError: %s' % x.get('type')

                                    if trace:
                                        cursor.execute("""INSERT INTO traceroutes (analysis_id, trace_id, src, dst, sport, dport, firsthop,
                                        hop_count, stop_reason, stop_data, TS, TS_epoch) VALUES """ + ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in trace))

                                    if hops:
                                        cursor.execute("""INSERT INTO hops (analysis_id, trace_id, hop_n, hop_ip, hop_ip_previous, rtt, reply_ttl,
                                        probe_size, reply_size, reply_ipid, reply_tos, icmp_type, icmp_code, icmp_q_ttl, icmp_q_len, icmp_q_tos,
                                        MPLS_label, MPLS_numlabel) VALUES """ + ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in hops))

                                    if ping_datos:
                                        cursor.execute("""INSERT INTO ping (analysis_id, ping_id, src, dst, TS_epoch, ping_sent, replies, loss, reply_ttl, ttl_unique) VALUES
                                        """ + ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in ping_datos))


                                stdout.write('\r\tCompleted..............'+str(cant_bloque_total)+'/'+str(cant_bloque_total))
                                stdout.flush()

                                cursor.execute('SELECT nodes_stored FROM analysis WHERE analysis_id=\'' + ID + '\';')
                                aux = cursor.fetchall()[0][0]
                                try:
                                    aux = aux + '|' + nodo
                                except:
                                    aux = nodo

                                cursor.execute('UPDATE analysis SET nodes_stored=\''+str(aux)+'\' WHERE analysis_id=\'' + ID + '\';')
                                cursor.execute('UPDATE analysis SET last_trace_id=\''+str(trace_id)+'\' WHERE analysis_id=\'' + ID + '\';')
                                cursor.execute('UPDATE analysis SET last_ping_id=\''+str(ping_id)+'\' WHERE analysis_id=\'' + ID + '\';')
                                cursor.execute('DELETE FROM nodes_working WHERE node=\''+str(nodo)+'\' AND analysis_id=\'' + ID + '\';')

                                conexion.commit()

                                command = 'parallel-ssh -H ' + str(nodo) + ' -l ' + slice_name + ' ' + 'sudo rm /home/'+slice_name+'/'+user+'/*_'+ID+'*'
                                proceso = Popen([command], shell=True, stdout=PIPE)
                                basura = proceso.stdout.readlines()
                                proceso.stdout.close()

                                nodos_preparados.append(nodo)

                                print  "\n\tDuration: ", (int(time()) - referencia) /60, "min\n"
                                print '\n\tTraceroutes correct: ', count_trace + 1, ' (',int((count_trace*100.0)/(count_trace + count_trace_error)),'%)'
                                print '\tTraceroutes fail: ', count_trace_error, ' (',int((count_trace_error*100.0)/(count_trace + count_trace_error)),'%)'
                                print '\n\t\tStored successful!\n'

                            else:
                                print '\nJSON file empty: %s' % nodo
                        else:
                            print 'JSON file not found'
                    else:
                        print '\nDecompressed file not found'
                else:
                    print '\nFile not found'


            except KeyboardInterrupt:
                conexion.rollback()
                if confirmar('Continue to next node'):
                    pass
                else:
                    raise

            except DatabaseError as e:
                print 'Error: %s' % str(e)
                conexion.rollback()
                raise

    except KeyboardInterrupt:
        pass

    except Exception as e:
        print 'ERROR:\n %s' % str(e)
        pass

    finally:
        vaciarTemp()
        if len(nodos_preparados) == len(nodos_almacenar):
            status = True
        else:
            status = False

    cursor.close()
    conexion.close()

    return status

def almacenar_mediciones_B2B_BD(auth, slice_name, ID, nodos_almacenar, sudoPassword = None):
    """ Store in DB the data of exploration in the nodes <nodos_almacenar> """

    user = auth.get('Username')

    nro_nodo = 0
    total_nodos = len(nodos_almacenar)
    nodos_preparados = []

    conexion = connect(conectar_BD())
    cursor = conexion.cursor()

    try:
        cursor.execute('SELECT max(trace_id) FROM traceroutes WHERE analysis_id=%s;' % ID)
        try:
            last_trace_id = int(cursor.fetchall()[0][0]) + 1
        except:
            last_trace_id = 1

        for nodo in nodos_almacenar:
            try:
                nro_nodo += 1
                print '\n\tNode ' + str(nro_nodo) + '/' + str(total_nodos) + ': ', nodo + '\n'

                vaciarTemp()

                print 'Copying files of results\n'
                comando = 'scp '+slice_name+'@'+nodo+':/home/'+slice_name+'/pamplona/resultados.tar.gz ' + directorio + '/temp/'
                call([comando], shell=True)

                if exists(directorio + '/temp/resultados.tar.gz'):
                    print '\nDecompressing files'
                    proceso = Popen(['cd ' + directorio + '/temp/; tar -zxf ' + directorio + '/temp/resultados.tar.gz'], shell=True)
                    proceso.wait()

                    if exists(directorio + '/temp/resultados_general.csv'):

                        print '\nWriting data'

                        trace = set()
                        hops = set()

                        # TRACEROUTE DATA
                        stdout.write('\r\tTraceroutes data:\t')
                        stdout.flush()
                        with open(directorio + '/temp/resultados_general.csv', 'r') as f:

                            countrdr = csv.DictReader(f, delimiter='\t')
                            numRegistros = len(list(countrdr))
                            n = 0
                            p_last = -1
                            f.seek(0)

                            reader = csv.DictReader(f, delimiter='\t')
                            for x in reader:
                                n += 1
                                try:
                                    p = str((float(n) / numRegistros) * 100)[:4]
                                except:    
                                    p = '0'

                                if p != p_last:
                                    stdout.write('\r\tTraceroutes data:\t %s%s' % (p,'%'))
                                    stdout.flush()
                                    P_last = p
                                
                                cursor.execute("INSERT INTO traceroutes (analysis_id, trace_id, src, dst, ts_epoch, path) VALUES (%s,%s,'%s','%s',%s,'%s')" % (ID, int(x.get('trace_id')) + last_trace_id, x.get('src'), x.get('dst'), x.get('ts_epoch'), x.get('path')) )

                            f.close()

                        # HOPS DATA
                        stdout.write('\n\r\tHops data:\t\t')
                        stdout.flush()
                        with open(directorio + '/temp/resultados_hops.csv', 'r') as f:

                            countrdr = csv.DictReader(f, delimiter='\t')
                            numRegistros = len(list(countrdr))
                            n = 0
                            p_last = -1
                            f.seek(0)

                            reader = csv.DictReader(f, delimiter='\t')
                            for x in reader:
                                n += 1
                                try:
                                    p = str((float(n) / numRegistros) * 100)[:4]
                                except:    
                                    p = '0'

                                if p != p_last:
                                    stdout.write('\r\tHops data:\t\t %s%s' % (p,'%'))
                                    stdout.flush()
                                    P_last = p

                                cursor.execute("INSERT INTO hops (analysis_id, trace_id, hop_n, hop_ip, rtt, reply_ttl) VALUES (%s,%s,%s,'%s',%s,%s)" % (ID, int(x.get('trace_id')) + last_trace_id, x.get('hop_n'), x.get('hop_ip'), x.get('rtt'), x.get('reply_ttl')) )

                            f.close()

                        cursor.execute('SELECT nodes_stored FROM analysis WHERE analysis_id=\'' + ID + '\';')
                        aux = cursor.fetchall()[0][0]
                        try:
                            aux = aux + '|' + nodo
                        except:
                            aux = nodo

                        cursor.execute('SELECT max(trace_id) FROM traceroutes WHERE analysis_id=%s;' % ID)
                        try:
                            last_trace_id = int(cursor.fetchall()[0][0]) + 1
                        except:
                            last_trace_id = 1

                        cursor.execute('UPDATE analysis SET nodes_stored=\''+str(aux)+'\' WHERE analysis_id=\'' + ID + '\';')
                        cursor.execute('UPDATE analysis SET last_trace_id=\''+str(last_trace_id)+'\' WHERE analysis_id=\'' + ID + '\';')
                        cursor.execute('DELETE FROM nodes_working WHERE node=\''+str(nodo)+'\' AND analysis_id=\'' + ID + '\';')

                        conexion.commit()
                        
                        command = 'parallel-ssh -H ' + str(nodo) + ' -l ' + slice_name + ' ' + 'sudo rm /home/'+slice_name+'/pamplona/resultados.tar.gz'
                        proceso = Popen([command], shell=True, stdout=PIPE)
                        basura = proceso.stdout.readlines()
                        proceso.stdout.close()

                        nodos_preparados.append(nodo)

                        print '\n\t\tStored successful!\n'

                    else:
                        print '\nDecompressed file not found'

                else:
                    print '\nFile not found'

            except KeyboardInterrupt:
                conexion.rollback()
                if confirmar('Continue to next node'):
                    pass
                else:
                    raise

            except DatabaseError as e:
                print 'Error: %s' % str(e)
                conexion.rollback()
                raise

    except KeyboardInterrupt:
        pass

    except Exception as e:
        print 'ERROR:\n %s' % str(e)
    finally:
        vaciarTemp()
        if len(nodos_preparados) == len(nodos_almacenar):
            status = True
        else:
            status = False

    cursor.close()
    conexion.close()

    return status
