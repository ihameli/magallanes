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

from os import system, remove, listdir
from os.path import exists, dirname, abspath
from sys import stdout
from time import sleep, time, asctime, gmtime
from psycopg2 import connect, Error as DatabaseError
from subprocess import PIPE, Popen, call
from geoip2.database import Reader as geoip2_reader

from func_BD import conectar_BD
from func_analysis import ingresarPassSudo
from func_admin import confirmar, verNodos

directorio = dirname(abspath(__file__))
archivo_conf = directorio + '/files/program.conf'
dir_temp = directorio + '/temp/direcciones'
script_dir = directorio + '/script_midar.py'
direccion_geolite = directorio + '/files/GeoLite2-City.mmdb'

# Read config file
config = {}
with open(archivo_conf) as f:
    for line in f:
        (key, value) = line.split(':')
        value = value.rstrip('\n')
        value = ''.join(e for e in value if e!=' ') 
        key = ''.join(e for e in key if e!=' ')         
        config[key] = value

mensajeErrorConexion = '\n--- Conexion Problem---\n'
mensajeEnter = '\n Press ENTER to continue \n'

class ErrorPlanetlab(Exception):
    """ Exceptions related to Planetlab API """
    pass

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

###############################################################################

def obtenerEstado(ID):
    """ Get resolution state of exploration <ID> """

    conexion = connect(conectar_BD())
    cursor = conexion.cursor()
    cursor.execute('SELECT topology_state FROM analysis where analysis_id=%s;' % ID)
    estado = cursor.fetchall()[0][0]
    cursor.close()
    conexion.close()

    return estado

def grabarEstado(midar_parametros, nuevo_estado):
    """ Save state of alias resolution process """

    # List of possible states:
        # 0: Topology unresolved
        # 1: Solving topology on a single node -> Previo a estado final
        # 2: Running estimation state across multiple nodes -> Estado intermedio
        # 3: Solving topology across multiple nodes -> Previo a estado final
        # 4: Topology resolved

    # connecting to DB
    conexion = connect(conectar_BD())
    cursor = conexion.cursor()

    # Update ANALYSIS table
    cursor.execute("UPDATE analysis SET topology_state='%s' WHERE analysis_id=%s;" % (nuevo_estado, midar_parametros.get('ID') ))

    # Update ALIAS_RESOLUTION table
    if nuevo_estado in ('1', '2'):
        query = """INSERT INTO alias_resolution(
            analysis_id,
            estado,
            nodes,
            mper_port,
            mper_pps,
            est_duration,
            est_rounds,
            elim_rounds,
            cor_rounds,
            est_overlap,
            disc_overlap,
            elim_overlap,
            cor_overlap,
            cor_concurrency,
            elim_concurrency,
            TS)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        datos = []

        datos.append((
            int(midar_parametros.get('ID')),
            nuevo_estado,
            ' '.join(midar_parametros.get('nodos_elegidos')),
            int(midar_parametros.get('config').get('mper_port')),
            int(midar_parametros.get('config').get('mper_pps')),
            int(midar_parametros.get('config').get('est_duration')),
            int(midar_parametros.get('config').get('est_rounds')),
            int(midar_parametros.get('config').get('elim_rounds')),
            int(midar_parametros.get('config').get('cor_rounds')),
            int(midar_parametros.get('config').get('est_overlap')),
            int(midar_parametros.get('config').get('disc_overlap')),
            int(midar_parametros.get('config').get('elim_overlap')),
            int(midar_parametros.get('config').get('cor_overlap')),
            midar_parametros.get('config').get('cor_concurrency'),
            midar_parametros.get('config').get('elim_concurrency'),
            asctime(gmtime(int(time())))))

        cursor.executemany(query, datos)

    elif nuevo_estado == '3':
        cursor.execute("UPDATE alias_resolution SET (estado, nodes) =('%s', '%s') WHERE analysis_id=%s;" % (nuevo_estado, ' '.join(midar_parametros.get('nodos_elegidos')), midar_parametros.get('ID') ))

    elif nuevo_estado == '4':
        cursor.execute("UPDATE alias_resolution SET estado='%s' WHERE analysis_id=%s;" % (nuevo_estado, midar_parametros.get('ID') ))


    conexion.commit()
    cursor.close()
    conexion.close()

    return None

def estadoNodos(midar_parametros, opcion, nodo):
    """ Manage nodes in alias resolution processes """

    conexion = connect(conectar_BD())
    cursor = conexion.cursor()

    ID = midar_parametros.get('ID')

    if opcion == 'reservar':
        cursor.execute("INSERT INTO nodes_working (node, analysis_id, work_type) VALUES ('%s', %s, '%s')" % (nodo, ID, 'MIDAR'))

    elif opcion == 'liberar':
        cursor.execute("DELETE FROM nodes_working WHERE analysis_id=%s and node='%s'" % (ID,nodo))

    conexion.commit()
    cursor.close()
    conexion.close()

    return None

def obtenerNodosUtilizables():
    """ Get useful nodes to alias resolution process, ordered by priority  """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        cursor.execute('SELECT node FROM nodes_priority_for_AR WHERE priority<>0 AND node not in (SELECT node FROM nodes_working) order by priority;')
        try:
            aux = cursor.fetchall()
            nodos = [x[0] for x in aux]
        except:
            nodos = []

        cursor.close()
        conexion.close()

    except DatabaseError as e:
        print 'Error: %s' % str(e)
        nodos = []

    return nodos


def generarListas(midar_parametros, IPs):
    """ List of IP to resolve """

    stdout.write('\n\r\tGenerating address list: ')
    stdout.flush()

    # Number of list to resolver
    cant_listas = len(IPs) / int(config['num_ip_list'])
    if len(IPs) % int(config['num_ip_list']) > 0:
        cant_listas += 1

    # Generate lists
    lista = [set() for x in range(cant_listas)]

    # Fill lists
    i = 0
    for x in IPs:
        lista[i].add(x)
        i += 1
        if i == cant_listas: i = 0

    # Generate files
    i = 1
    for IP in lista:
        nombre_archivo = '%s_%s_%s.temp' % (dir_temp, midar_parametros.get('ID'), str(i))
        midar_parametros['nombre_lista'].append(nombre_archivo)
        archivo = open(nombre_archivo, 'a')
        [archivo.write(str(x) + '\n') for x in IP]
        archivo.close()
        i += 1

    stdout.write('\r\tGenerating address list: Done\n\n')
    stdout.flush()

    return None

def generar_listas_v2(midar_parametros, IPs):
    """ List of IP to resolve clasified by probe to used """

    indice_inicial = len(midar_parametros.get('nombre_lista'))

    stdout.write('\n\r\tGenerating address list: ')
    stdout.flush()

    # Dictionary for addresses
    direcciones = {}

    # Keys for dictionary
    claves = set()
    [claves.add(x.split('.')[0]) for x in IPs]

    for x in claves:
        direcciones[x] = set()

    [direcciones[x.split('.')[0]].add(x) for x in IPs]

    # List of IP sets
    listas = []
    listas.append(set())

    for x in direcciones.keys(): # IP address Blocks /8

        nuevo_bloque = direcciones.get(x)
        indice_listas = len(listas)
        crear_lista = True

        for x in range(indice_listas): # Existing lists

            if len(listas[x]) + len(nuevo_bloque) <= int(config['num_ip_list']):
                listas[x] = listas[x] | nuevo_bloque
                crear_lista = False
                break

        if crear_lista: # If the block does not fit into any existing genre lists then it use a new list
            if len(nuevo_bloque) > int(config['num_ip_list']):
                nuevo_bloque = set( list(nuevo_bloque)[:int(config['num_ip_list'])] )
            listas.append(set(nuevo_bloque))

    # Generate files to transfer
    i = indice_inicial + 1
    for IP in listas:
        nombre_archivo = '%s_%s_%s.temp' % (dir_temp, midar_parametros.get('ID'), str(i))
        midar_parametros['nombre_lista'].append(nombre_archivo)
        archivo = open(nombre_archivo, 'a')
        [archivo.write(str(x) + '\n') for x in IP]
        archivo.close()
        i += 1

    stdout.write('\r\tGenerating address list: Listo\n\n')
    stdout.flush()

    return None


def vaciarTemp():
    """ """

    archivos = [x for x in listdir('%s/temp' % directorio)]

    for x in archivos:
        remove('%s/temp/%s' % (directorio, x))

    return None


def comprobarMidarInstancia(nodo, slice_name):
    """ Verify if MIDAR is running in a node """

    proceso = Popen(['parallel-ssh', '-P', '-H', nodo, '-l', slice_name, 'ls'], stdout=PIPE)
    salida = proceso.stdout.read()
    proceso.stdout.close()

    if 'MIDAR_WORKING' in salida:
        resultado = False
    else:
        resultado = True

    return resultado

def eliminarArchivosTransferidos(midar_parametros, slice_name, opcion):
    """ Remove transferred files """

    print 'Deleting files on remote nodes'
    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        nodos_transferidos = midar_parametros.get(opcion)
        for nodo in nodos_transferidos:
            proceso = Popen(['parallel-ssh', '-H', nodo, '-l', slice_name, 'sudo rm /home/%s/%s/*' % (slice_name, midar_parametros.get('user'))], stdout=PIPE)
            proceso.stdout.close()
            cursor.execute("DELETE FROM ip_resolution WHERE analysis_id=%s and node='%s' " % (midar_parametros.get('ID'),nodo))

    except Exception, e:
        print 'ERROR: %s' % str(e)
        conexion.rollback()

    else:
        conexion.commit()

    finally:
        cursor.close()
        conexion.close()

    return None

def obtenerLocalizacionRouters(ID):
    """ Get location of each router """

    stdout.write('\n\r\tGetting location of routers: ')
    stdout.flush()

    try:
        # Get ROUTER ID of analysis
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        cursor.execute(""" SELECT DISTINCT ON (i.router_id) i.router_id, i.IP
                           FROM  interfaces as i left join routers as r 
                             ON  i.analysis_id = r.analysis_id and i.router_id=r.router_id
                           WHERE i.analysis_id=%s and r.cod_continent is null;""" % ID)
        RID = cursor.fetchall()
        cursor.close()
        conexion.close()
        
    except Exception as e:
        print '\nERROR: %s' % str(e)

    else:
        # Connecting to MaxMind DB
        reader = geoip2_reader(direccion_geolite)
        datos = set()
        N = int(len(RID) / 1000) + 1
        n = 1
        
        # Getting an interface for each router ID
        for r in RID:
            n = n + 1
            x  = r[0]
            IP = r[1]
            
            try:
                # Taking an IP to get its location
                response = reader.city(IP)

                try:
                    continent = str(response.continent.name)
                except UnicodeEncodeError:
                    continent = None

                try:
                    cod_continent = str(response.continent.code)
                except UnicodeEncodeError:
                    cod_continent = None

                try:
                    country = str(response.country.name)
                except UnicodeEncodeError:
                    country = None

                try:
                    cod_country = str(response.country.iso_code)
                except UnicodeEncodeError:
                    cod_country = None

                try:
                    region = str(response.subdivisions.most_specific.name)
                    region = region.translate(None, "'")
                except UnicodeEncodeError:
                    region = None

                try:
                    cod_region = str(response.subdivisions.most_specific.iso_code)
                except UnicodeEncodeError:
                    cod_region = None

                try:
                    city = str(response.city.name)
                    city = city.translate(None, "'")
                except UnicodeEncodeError:
                    city = None

                try:
                    latitude = float(response.location.latitude)
                except:
                    latitude = 0

                try:
                    longitude = float(response.location.longitude)
                except:
                    longitude = 0

                localizacion = (continent, cod_continent, country, cod_country, region, cod_region, city, latitude, longitude, x)

                datos.add(localizacion)
                
            except Exception as e:
                conexion.rollback()

            else:
                conexion.commit()
        
        reader.close()
        
        try:
            if len(datos) > 0:
                conexion = connect(conectar_BD())
                cursor = conexion.cursor()
                
                N = len(datos)
                n = 1
                datos = list(datos)
                for x in datos:
                    n = n + 1
                    query = "UPDATE routers SET (continent, cod_continent, country, cod_country, region, cod_region, city, latitude, longitude) = ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s) WHERE analysis_id=%s and router_id=%s;" % (ID, x) 
                    cursor.execute(query)
                    if (n % 100) == 0: conexion.commit() 
                conexion.commit()
                
                cursor.close()
                conexion.close()

        except Exception as e:
            print str(e)
            pass
            
    finally:
        stdout.write('\r\tGetting location of routers: Done')
        stdout.flush()

    return None


def transferirArchivos(midar_parametros, slice_name):
    """ Copy files to remote nodes """

    #
    print '\nTransferring files (%s):\n' % (str(len(midar_parametros.get('nombre_lista'))))

    # Taking list of files to transfer
    nodos_disponibles = obtenerNodosUtilizables()

    # Max number of attempts to transfer the files to a particular node
    num_intentos = 3

    # Startig to transfer files
    for archivo_direcciones in midar_parametros.get('nombre_lista'):
        ejecutar = True
        try:
            while ejecutar:

                intento = 1
                nodo = nodos_disponibles.pop(0)

                stdout.write('\r\tNode: %s\t\t' % nodo)
                stdout.flush()

                # Verifico que no se esté ejecutando el MIDAR en el momento actual
                if comprobarMidarInstancia(nodo, slice_name):
                    while intento <= num_intentos:

                        # Creo la carpeta contenedora
                        proceso = Popen(['parallel-ssh', '-H', nodo, '-l', slice_name, 'mkdir ~/%s' % midar_parametros.get('user')], stdout=PIPE)
                        salida = proceso.stdout.read()
                        proceso.stdout.close()

                        # Transfiero lista de direcciones
                        proceso = Popen(['parallel-scp','-H', nodo , '-l', slice_name, archivo_direcciones, '/home/%s/%s/' % (slice_name, midar_parametros.get('user'))], stdout=PIPE)
                        salida = proceso.stdout.read()
                        proceso.stdout.close()

                        if '[SUCCESS]' in salida:
                            # Transfiero script
                            proceso = Popen(['parallel-scp','-H', nodo , '-l', slice_name, script_dir, '/home/%s/%s/' % (slice_name, midar_parametros.get('user'))], stdout=PIPE)
                            salida = proceso.stdout.read()
                            proceso.stdout.close()

                            if '[SUCCESS]' in salida:
                                registrar_ip_nodo(midar_parametros.get('ID'), nodo, archivo_direcciones)
                                midar_parametros['nodos_elegidos'].add(nodo)
                                ejecutar = False
                                mensaje = 'SUCCESS'
                                break
                            else:
                                intento += 1
                                mensaje = 'FAILED'
                else:
                    mensaje = 'Busy'

                stdout.write('\r\tNode: %s\t\t%s' % (nodo, mensaje))
                stdout.flush()
                print '\n'

        except IndexError:
            raise

    return None

def registrar_ip_nodo(ID, nodo, archivo_direcciones):
    """ Write the IPs on each remote node """

    try:
        # Read IP list
        archivo = open(archivo_direcciones)
        lista_ip = archivo.readlines()
        archivo.close()

        # Generate register
        datos = [(ID , nodo, ip.replace("\n", '')) for ip in lista_ip]

        # Writing table
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        query = """INSERT INTO ip_resolution (analysis_id, node, ip) VALUES (%s, %s, %s)"""
        cursor.executemany(query, datos)
        conexion.commit()
        cursor.close()
        conexion.close()

    except Exception as e:
        print 'ERROR: %s' % str(e)

    return None

def ingresarComandoMidar(texto, defecto):
    """ Request values to configure Midar """

    while True:
        opcion = raw_input(texto + ':\t')
        try:
            if opcion == '':
                salida = str(defecto)
                break
            elif int(opcion) > 0:
                salida = opcion
                break
            else:
                print 'Wrong'
        except:
            print 'Wrong'

    return salida

def configuracion_MIDAR(midar_parametros):
    """ Get values to configure Midar """

    cantidad = len(midar_parametros.get('IPs'))/len(midar_parametros.get('nombre_lista'))

    #--run-id=<RUN_ID>      label for this experiment, e.g. start date (REQUIRED)
    midar_parametros['config']['run_id'] = midar_parametros.get('ID')

    #--mper-port=<PORT>     connect to mper on <PORT> [8746]
    midar_parametros['config']['mper_port'] = '8746'

    #--mper-pps=<N>         assume mper sends <N> pkts per second [100]
    pps_recomendado = str(min(max(cantidad/500,20),100))

    while True:
        midar_parametros['config']['mper_pps'] = ingresarComandoMidar('pps [Recomenado '  + pps_recomendado+ ']', pps_recomendado)
        if int(midar_parametros.get('config').get('mper_pps')) < 20 or int(midar_parametros.get('config').get('mper_pps')) > 100:
            print 'Valor fuera de limite (20 - 100)'
        else:
            break

    #--est-duration=<N>     duration of Estimation rounds, in seconds [10]
    midar_parametros['config']['est_duration'] = ingresarComandoMidar('duration of Estimation rounds, in seconds [10]', 10)

    #--est-rounds=<N>       number of probing rounds in Estimation [30]
    midar_parametros['config']['est_rounds'] =  ingresarComandoMidar('number of probing rounds in Estimation [30]', 30)

    #--elim-rounds=<N>      number of probing rounds in Elimination [10]
    midar_parametros['config']['elim_rounds'] = ingresarComandoMidar('number of probing rounds in Elimination [10]', 10)

    #--cor-rounds=<N>       number of probing rounds in Corroboration [10]
    midar_parametros['config']['cor_rounds'] = ingresarComandoMidar('number of probing rounds in Corroboration [10]', 10)

    #--est-overlap=<N>      samples needed by MBT in Estimation [10]
    midar_parametros['config']['est_overlap'] = ingresarComandoMidar('samples needed by MBT in Estimation [10]', 10)

    #--disc-overlap=<N>     samples needed by MBT in Discovery [5]
    midar_parametros['config']['disc_overlap'] = ingresarComandoMidar('samples needed by MBT in Discovery [5]', 5)

    #--elim-overlap=<N>     samples needed by MBT in Elimination [5]
    midar_parametros['config']['elim_overlap'] = ingresarComandoMidar('samples needed by MBT in Elimination [5]', 5)

    #--cor-overlap=<N>      samples needed by MBT in Corroboration [5]
    midar_parametros['config']['cor_overlap'] = ingresarComandoMidar('samples needed by MBT in Corroboration [5]', 5)

    #--elim-concurrency=<N> subsets to probe in parallel in Elimination [default is based on target list size and mper-pps]
    midar_parametros['config']['elim_concurrency'] = None
    try:
        midar_parametros['config']['cor_concurrency'] = int(midar_parametros.get('config').get('cor_concurrency'))
    except:
        pass

    #--cor-concurrency=<N>  subsets to probe in parallel in Corroboration [default is based on target list size and mper-pps]
    midar_parametros['config']['cor_concurrency'] = None
    try:
        midar_parametros['config']['elim_concurrency'] = int(midar_parametros.get('config').get('elim_concurrency'))
    except:
        pass

    return None

def iniciarProceso(midar_parametros, slice_name, opcion):
    """ Start process on remote nodes """

    resultado = True

    instalacion = set()

    #
    print '\nStarting process:\n'

    for n, nodo in enumerate(midar_parametros.get('nodos_elegidos')):

        comando = 'ssh -f -l %s %s \"cd /home/%s/%s/; nohup python script_midar.py %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s > /dev/null  2> /dev/null\"' % (
            slice_name,
            nodo,
            slice_name,
            midar_parametros.get('user'),
            slice_name,
            nodo,
            midar_parametros.get('user'),
            midar_parametros.get('ID'),
            opcion,
            midar_parametros.get('config').get('mper_port'),
            midar_parametros.get('config').get('mper_pps'),
            midar_parametros.get('config').get('est_duration'),
            midar_parametros.get('config').get('est_rounds'),
            midar_parametros.get('config').get('elim_rounds'),
            midar_parametros.get('config').get('cor_rounds'),
            midar_parametros.get('config').get('est_overlap'),
            midar_parametros.get('config').get('disc_overlap'),
            midar_parametros.get('config').get('elim_overlap'),
            midar_parametros.get('config').get('cor_overlap'))

        intento = 0
        num_intentos = 3

        while True:
            # Running script
            if call([comando], shell=True) == 0:
                instalacion.add(nodo)
                break

            else:
                intento += 1
                if intento > num_intentos:
                    break
                else:
                    sleep(2)

        if nodo in instalacion:
            print '\t %s:\t Process initiated' % nodo
            estadoNodos(midar_parametros, 'reservar', nodo)

        else:
            resultado = False
            print '\t %s:\t Cannot iniciate process' % nodo

    return resultado

def asociar_IP_router(tabla_interfaces, ID, IP, max_router_id, datos_routers, datos_interfaces):
    """ Link each IP to a router """

    if tabla_interfaces.has_key(IP):
        router_id = tabla_interfaces.get(IP)

    else:
        max_router_id += 1
        router_id = max_router_id
        tabla_interfaces[IP] = router_id
        datos_routers.add((ID, router_id, 1))
        datos_interfaces.add((ID, router_id, IP))

    return router_id, max_router_id

def resolverEnlaces(ID):
    """ Find the router links based on the results of alias resolution process """

    stdout.write('\n\r\tFinding links:\t')
    stdout.flush()

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        # Getting max router_id
        cursor.execute('SELECT max(router_id) FROM routers WHERE analysis_id=%s;' % ID)

        try:
            max_router_id = int(cursor.fetchone()[0])
        except:
            max_router_id = 1

        # Loading in memory INTERFACE table
        tabla_interfaces = {}
        cursor.execute('SELECT ip, router_id FROM interfaces WHERE analysis_id=%s;' % ID)
        reg = cursor.fetchone()

        while reg:
            tabla_interfaces[reg[0]] = reg[1]
            reg = cursor.fetchone()

        # Finding out neighbour IPs
        cursor.execute('SELECT (IP1, IP2) FROM links_IP WHERE analysis_id=%s;' % ID)

        # Checking each pair of neighboring IPs
        enlaces = set()
        datos_routers = set()
        datos_interfaces = set()
        x = cursor.fetchone()

        while x:

            IP = x[0][1:-1].split(',')

            # Analyze each IP to get to each router belongs
            r1, max_router_id = asociar_IP_router(tabla_interfaces, ID, IP[0], max_router_id, datos_routers, datos_interfaces)
            r2, max_router_id = asociar_IP_router(tabla_interfaces, ID, IP[1], max_router_id, datos_routers, datos_interfaces)

            # Save the router link
            enlaces.add((int(ID), r1, r2))

            # move to next link
            x = cursor.fetchone()

        # Insert new registers on ROUTER table
        query = """INSERT INTO routers (analysis_id, router_id, num_interfaces) VALUES (%s, %s, %s)"""
        cursor.executemany(query, list(datos_routers))

        # Insert new registers on INTERFACES table
        query = """INSERT INTO interfaces (analysis_id, router_id, ip) VALUES (%s, %s, %s)"""
        cursor.executemany(query, list(datos_interfaces))

        # Insert new registers on LINKS table
        query = """INSERT INTO links (analysis_id, R1, R2) VALUES (%s, %s, %s)"""
        cursor.executemany(query, list(enlaces))

        conexion.commit()

        stdout.write('\r\tFinding links:\tLinks created')
        stdout.flush()

        cursor.close()
        conexion.close()

    except Exception as e:
        stdout.write('\r\tFinding links:\tERROR - %s' % e)
        stdout.flush()
        conexion.rollback()

    return None


def descargarResultados(slice_name, midar_parametros):
    """ Download results  """

    ID = midar_parametros.get('ID')

    user= midar_parametros.get('user')

    resultado = True

    vaciarTemp()

    # Getting nodes where the process was installed
    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        cursor.execute('SELECT nodes FROM alias_resolution WHERE analysis_id=%s;' % ID)
        nodos = cursor.fetchall()[0][0]
        cursor.close()
        conexion.close()
        nodos = nodos.split()

    except Exception as e:
        print 'ERROR: %s' % str(e)
        resultado = False

    else:
        nodos_OK = set()
        nodos_KO = set()
        resultado = True
        try:
            print 'Getting results\n'
            conexion = connect(conectar_BD())
            cursor = conexion.cursor()
            for nodo in nodos:
                print '\n\nNode: %s' % nodo
                archivo = '/home/%s/%s/resultados__%s__%s__.txt.gz' % (slice_name, user, ID, nodo)
                comando = 'scp %s@%s:%s %s/temp/' % (slice_name, nodo, archivo, directorio)
                call([comando], shell=True)
                if exists('%s/temp/resultados__%s__%s__.txt.gz' % (directorio, ID, nodo)):
                    nodos_OK.add(nodo)
                    proceso = Popen(['gzip -d %s/temp/resultados__%s__%s__.txt.gz' % (directorio, ID, nodo)], shell=True)
                    proceso.wait()
                    cursor.execute("DELETE FROM ip_resolution WHERE analysis_id=%s AND node='%s' " % (ID,nodo))
                    estadoNodos(midar_parametros, 'liberar', nodo)
                    eliminarResultadosNodos(slice_name, user, nodo)
                else:
                    nodos_KO.add(nodo)

        except Exception as e:
            print 'ERROR: %s' % str(e)
            conexion.rollback()
            resultado = False
            vaciarTemp()

        else:
            conexion.commit()
            if len(nodos_KO) == 0:
                print '\nAll results were correctly downloaded\n'
                print 'TOTAL: %s/%s' % (len(nodos_OK), len(nodos))
            else:
                print '\nThere have been results without download:\n'
                print 'TOTAL: %s/%s' % (len(nodos_OK), len(nodos))
                if confirmar('Discard results'):
                    cursor.execute("DELETE FROM ip_resolution WHERE analysis_id=%s" % ID)
                    conexion.commit()
                    for nodo in nodos_KO:
                        eliminarResultadosNodos(slice_name, user, nodo)
                        estadoNodos(midar_parametros, 'liberar', nodo)
                    nodos_KO.clear()

        finally:
            cursor.execute("UPDATE alias_resolution SET nodes='%s' WHERE analysis_id=%s;" % ( ' '.join(nodos_KO), ID ))
            conexion.commit()
            cursor.close()
            conexion.close()

    return resultado


def almacenarResultados(ID, MIDAR_folder = 1):
    """ Store results """

    try:
        print '\nStoring results'

        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        # Find MIDAR folder
        if MIDAR_folder == 1:
            archivo_resultados = [x for x in listdir('%s/temp' % directorio) if 'resultados__' in x]
        else:
            archivo_resultados = [x for x in listdir('%s/temp' % directorio)]


        # Analysis each result file
        for x in archivo_resultados:

            # Load result file
            archivo = open('%s/temp/%s' % (directorio, x))
            resultado_aliases = archivo.readlines()
            archivo.close()

            remove('%s/temp/%s' % (directorio, x))

            # Remove unnecessary lines
            resultado_aliases.pop(0)
            resultado_aliases.pop(0)
            resultado_aliases.pop(0)
            resultado_aliases.pop(0)
            resultado_aliases.pop(0)
            resultado_aliases = ' '.join(resultado_aliases)
            resultado_aliases = resultado_aliases.split('#')
            resultado_aliases.pop(0)
            resultado_aliases.pop()

            datos = []
            datos_int = []

            # Find max router_id
            cursor.execute('SELECT max(router_id) FROM routers WHERE analysis_id=%s;' % ID)

            try:
                router_id = int(cursor.fetchone()[0])
            except:
                router_id = 0

            # Analysis each router
            for x in resultado_aliases:
                x = x.split('\n')
                header = x.pop(0)
                header = header.split()
                x.pop()
                try:
                    router_id += 1
                    num_interfaces = int(header[2])

                except:
                    pass

                else:
                    [datos_int.append((int(ID), router_id, ip.strip(' '))) for ip in x]
                    datos.append((int(ID), router_id, num_interfaces))

            # Store processed results
            query = """INSERT INTO routers (analysis_id, router_id, num_interfaces) VALUES (%s, %s, %s)"""
            cursor.executemany(query, datos)

            query = """INSERT INTO interfaces (analysis_id, router_id, IP ) VALUES (%s, %s, %s)"""
            cursor.executemany(query, datos_int)

    except Exception as e:
        print 'ERROR: %s' % str(e)
        conexion.rollback()
        resultado = False

    else:
        conexion.commit()
        resultado = True

    finally:
        cursor.close()
        conexion.close()

    return resultado

def eliminarResultadosNodos(slile_name, user, nodo):
    """ Remove files and processes on remote node """

    comando = 'ssh -f -l %s %s \"sudo killall -I midar ping mper > /dev/null  2> /dev/null\"' % (slile_name, nodo)
    call([comando], shell=True)

    comando = 'ssh -f -l %s %s \"sudo rm /home/%s/MIDAR_WORKING /home/%s/%s/resultados__* /home/%s/%s/MIDAR.log /home/%s/%s/script_midar.py > /dev/null  2> /dev/null\"' % (slile_name, nodo, slile_name, slile_name, user, slile_name, user, slile_name, user)
    call([comando], shell=True)

    return None

def generarNuevasListas(midar_parametros):
    """ Generate new list base on the probe type to use """

    TCP = set()
    UDP = set()
    ICMP = set()
    indirect = set()
    sin_resolver = set()

    # Get IP-METODO pairs
    conexion = connect(conectar_BD())
    cursor = conexion.cursor()
    cursor.execute('SELECT IP, metodo FROM ip_metodo_midar WHERE analysis_id=%s;' % midar_parametros.get('ID'))
    salida = cursor.fetchall()
    cursor.close()
    conexion.close()

    # Assign each IP to their preferred method
    for x in salida:
        if x[1] == 'tcp':
            TCP.add(x[0])
        elif x[1] == 'udp':
            UDP.add(x[0])
        elif x[1] == 'icmp':
            ICMP.add(x[0])
        elif x[1] == 'indirect':
            indirect.add(x[0])
        else:
            sin_resolver.add(x[0])

    # Generate new list base on the prefered method of each IP
    if TCP:
        generar_listas_v2(midar_parametros, TCP)
    if UDP:
        generar_listas_v2(midar_parametros, UDP)
    if ICMP:
        generar_listas_v2(midar_parametros, ICMP)
    if indirect:
        generar_listas_v2(midar_parametros, indirect)

    return None

def almacenarMetodoPreferido(ID):
    """ Store the prefered method of each IP based on the result of estimete stage of MIDAR """

    try:
        archivo_resultados = [x for x in listdir('%s/temp' % directorio) if 'resultados__' in x]

        ip_metodo = set()

        # Read each file
        for x in archivo_resultados:
            archivo = open('%s/temp/%s' % (directorio, x))
            resultado_aliases = archivo.readlines()
            archivo.close()
            # Get IP_METODO
            for x in resultado_aliases[2:-1]:
                x = x.split()
                if x[1][-1]=='*':
                    ip_metodo.add((ID, x[0], x[1][0:-1]))

        # Write IP-METODO on ip_metodo_midar table
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        cursor.executemany("""INSERT INTO ip_metodo_midar (analysis_id, ip, metodo) VALUES (%s, %s, %s)""", ip_metodo)
        conexion.commit()
        cursor.close()
        conexion.close()
    except:
        resultado = False
    else:
        resultado = True

    return resultado

def buscar_ip_metodo(ID):
    """ Get the prefered method of each IP from ip_metodo_midar table """

    conexion = connect(conectar_BD())
    cursor = conexion.cursor()
    cursor.execute('SELECT * FROM ip_metodo_midar where analysis_id=%s limit 1;' % ID)
    salida = cursor.fetchall()
    cursor.close()
    conexion.close()

    if len(salida):
        resultado = True
    else:
        resultado = False

    return resultado


def cargarConfiguracionMIDAR(midar_parametros):
    """ """

    # Get the original configuration
    stdout.write('\n\r\tLoading configuration: ')
    stdout.flush()

    conexion = connect(conectar_BD())
    cursor = conexion.cursor()
    cursor.execute("""SELECT mper_port, mper_pps, est_duration, est_rounds, elim_rounds,
                            cor_rounds, est_overlap, disc_overlap, elim_overlap, cor_overlap,
                            cor_concurrency, elim_concurrency FROM alias_resolution where analysis_id=%s""" % midar_parametros.get('ID'))
    config = cursor.fetchall()[0]
    cursor.close()
    conexion.close()

    stdout.write('\r\tLoading configuration: Done\n')
    stdout.flush()

    # Apply configuration
    midar_parametros['config']['run_id'] = midar_parametros.get('ID')
    midar_parametros['config']['mper_port'] = config[0]
    midar_parametros['config']['mper_pps'] = config[1]
    midar_parametros['config']['est_duration'] = config[2]
    midar_parametros['config']['est_rounds'] = config[3]
    midar_parametros['config']['elim_rounds'] = config[4]
    midar_parametros['config']['cor_rounds'] = config[5]
    midar_parametros['config']['est_overlap'] = config[6]
    midar_parametros['config']['disc_overlap'] = config[7]
    midar_parametros['config']['elim_overlap'] = config[8]
    midar_parametros['config']['cor_overlap'] = config[9]
    midar_parametros['config']['elim_concurrency'] = config[10]
    midar_parametros['config']['cor_concurrency'] = config[11]

    return None

def hallarIP(ID):
    """ Find all distinct IP from the exploration """

    # Check if the IP have been already found yet
    conexion = connect(conectar_BD())
    cursor = conexion.cursor()

    cursor.execute('select ip_found from ip_found where analysis_id=%s limit 1;' % ID)

    aux = cursor.fetchall()

    cursor.close()
    conexion.close()

    # If not the I found it
    if not aux:
        stdout.write('\r\tFinding IP found: ')
        stdout.flush()

        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        cursor.execute('insert into ip_found select distinct(analysis_id), hop_ip from hops where analysis_id=%s;' % ID)
        conexion.commit()
        cursor.close()
        conexion.close()

        stdout.write('\r\tFinding IP found: Done\n')
        stdout.flush()

    return None

def hallarEnlacesIP(ID):
    """ Find IP links """

    # Checking if links have been already found
    conexion = connect(conectar_BD())
    cursor = conexion.cursor()

    cursor.execute('SELECT IP1, IP2 FROM links_IP WHERE analysis_id=%s LIMIT 1;' % ID)

    aux = cursor.fetchall()

    cursor.close()
    conexion.close()

    # If not the I found it
    if not aux:
        stdout.write('\r\tFinding IP links: ')
        stdout.flush()

        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        cursor.execute('insert into links_IP select distinct(analysis_id), HOP_IP_previous, HOP_IP from hops where analysis_id=%s and HOP_IP_previous is not null and HOP_IP is not null;' % ID)
        conexion.commit()
        cursor.close()
        conexion.close()

        stdout.write('\r\tFinding IP links: Done\n')
        stdout.flush()

    return None

def comprobarResultadosPendientes(midar_parametros, slice_name, estado):
    """ Check if there are pending results """

    user = midar_parametros.get('user')

    ID = midar_parametros.get('ID')

    # Checking if there are registers in ip_resolution table
    conexion = connect(conectar_BD())
    cursor = conexion.cursor()

    cursor.execute('SELECT distinct(ip) FROM ip_resolution WHERE analysis_id=%s order by 1;' % ID)
    aux = cursor.fetchall()

    lista_ip = [x[0] for x in aux]

    cursor.execute('SELECT distinct(node) FROM ip_resolution WHERE analysis_id=%s;' % ID)
    aux = cursor.fetchall()

    lista_nodos = [x[0] for x in aux]

    cursor.close()
    conexion.close()

    # IF there are then ask if reprocess or not
    if lista_ip:
        continuar = False
        print '\nQuedan IP sin resolver: '
        if confirmar('Reprocesar'):
            # Delete process in node
            for nodo in lista_nodos:
                eliminarResultadosNodos(slice_name, user, nodo)
                estadoNodos(midar_parametros, 'liberar', nodo)

            # Reprocess IPs
            conexion = connect(conectar_BD())
            cursor = conexion.cursor()
            cursor.execute("DELETE FROM ip_resolution WHERE analysis_id=%s" % ID)
            conexion.commit()
            cursor.close()
            conexion.close()

            vaciarTemp()
            generarListas(midar_parametros, lista_ip)
            cargarConfiguracionMIDAR(midar_parametros)
            transferirArchivos(midar_parametros, slice_name)

            if estado == '2':
                iniciarProceso(midar_parametros, slice_name, 'estimacion')
            else:
                iniciarProceso(midar_parametros, slice_name, 'final')

            conexion = connect(conectar_BD())
            cursor = conexion.cursor()
            cursor.execute("UPDATE alias_resolution SET (estado, nodes) = ('%s', '%s') WHERE analysis_id=%s;" % (estado, ' '.join(midar_parametros.get('nodos_elegidos')), ID ))
            conexion.commit()
            cursor.close()
            conexion.close()
            #

    else:
        continuar = True

    return continuar


def darPrioridadNodos(nodos_midar):
    """ Prioritize nodes """

    prioridad_por_defecto = 10

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        # Read nodes in priority table
        cursor.execute('SELECT node FROM nodes_priority_for_AR;')
        try:
            aux = cursor.fetchall()
            nodos = [x[0] for x in aux]

        except:
            nodos = []

        finally:
            # Get the nodes that are not in table
            nodos_insertar = set(nodos_midar) - set(nodos)

            # Add default priority to nodes which are not in the table
            if nodos_insertar:
                datos = [(nodo, prioridad_por_defecto) for nodo in nodos_insertar]
            else:
                datos = []

        cursor.executemany("""INSERT INTO nodes_priority_for_AR (node, priority) VALUES (%s, %s)""", datos)
        conexion.commit()

    except DatabaseError as e:
        print 'Error: BD - %s' % e

    else:
        cursor.close()
        conexion.close()

    return None

def programarResolucionAliases(ID, api_server, auth, slice_name, nodos_midar, estado):
    """ main function in alias resolution process """

    # START
    print '\nAlias resolution\n'

    # Define the dictionary to store parameters used
    midar_parametros = {}
    midar_parametros['ID'] = str(ID)
    midar_parametros['user'] = auth.get('Username')
    midar_parametros['nodos_elegidos'] = set() # Nodes where alias resolution process will run
    midar_parametros['IPs'] = set() # IPs to resolve
    midar_parametros['nombre_lista'] = []
    midar_parametros['config'] = {}

    darPrioridadNodos(nodos_midar)
    hallarIP(midar_parametros.get('ID'))
    hallarEnlacesIP(midar_parametros.get('ID'))

    if estado == '0':
        # Get the number of IPs to resolve
        stdout.write('\r\tLoading IPs: ')
        stdout.flush()

        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        cursor.execute('select ip_found from ip_found where analysis_id=%s order by ip_found;' % midar_parametros.get('ID'))

        [midar_parametros['IPs'].add(x[0]) for x in cursor]

        cursor.close()
        conexion.close()

        stdout.write('\r\tLoading IPs: %d \n' % len(midar_parametros.get('IPs')))
        stdout.flush()

        if True:

            # Removing files in Temp folder
            vaciarTemp()

            # Generting list of IP to resolver in remote nodes
            generarListas(midar_parametros, midar_parametros.get('IPs'))

            # Setting MIDAR
            configuracion_MIDAR(midar_parametros)

            if confirmar('Start process'):
                try:
                    # Transfering list to nodes
                    transferirArchivos(midar_parametros, slice_name)

                except IndexError:
                    print 'There is not enough not available nodes'
                    eliminarArchivosTransferidos(midar_parametros, slice_name, 'nodos_elegidos')

                except Exception, e:
                    print 'Error:\n', str(e)
                    eliminarArchivosTransferidos(midar_parametros, slice_name, 'nodos_elegidos')

                else:
                    ### Selecting method based on number of list generated previously
                    try:
                        if len(midar_parametros.get('nombre_lista')) == 1:
                            if iniciarProceso(midar_parametros, slice_name, 'final'):
                                grabarEstado(midar_parametros, '1')

                        else:
                            if iniciarProceso(midar_parametros, slice_name, 'estimacion'):
                                grabarEstado(midar_parametros, '2')

                    except Exception as e:
                        print 'Error:\n', str(e)
                        raise

                finally:
                    vaciarTemp()
        else:
            print 'There is not enough not available nodes'

    elif estado == '1':
        if descargarResultados(slice_name, midar_parametros):
            if almacenarResultados(midar_parametros.get('ID')):
                if comprobarResultadosPendientes(midar_parametros, slice_name, estado):
                    resolverEnlaces(midar_parametros.get('ID'))
                    obtenerLocalizacionRouters(midar_parametros.get('ID'))
                    grabarEstado(midar_parametros, '4')

    elif estado == '2':
        if descargarResultados(slice_name, midar_parametros):
            if almacenarMetodoPreferido(midar_parametros.get('ID')):
                if comprobarResultadosPendientes(midar_parametros, slice_name, estado):
                    vaciarTemp()
                    generarNuevasListas(midar_parametros)
                    cargarConfiguracionMIDAR(midar_parametros)
                    try:
                        transferirArchivos(midar_parametros, slice_name)
                        if iniciarProceso(midar_parametros, slice_name, 'final'):
                            grabarEstado(midar_parametros, '3')

                    except IndexError:
                        print 'There is not enough not available nodes'
                        eliminarArchivosTransferidos(midar_parametros, slice_name, 'nodos_elegidos')

                    except Exception, e:
                        print 'Error:\n', str(e)
                        eliminarArchivosTransferidos(midar_parametros, slice_name, 'nodos_elegidos')

                    finally:
                        vaciarTemp()

    elif estado == '3':
        if descargarResultados(slice_name, midar_parametros):
            if almacenarResultados(midar_parametros.get('ID'), 2):
                if comprobarResultadosPendientes(midar_parametros, slice_name, estado):
                    resolverEnlaces(midar_parametros.get('ID'))
                    obtenerLocalizacionRouters(midar_parametros.get('ID'))
                    grabarEstado(midar_parametros, '4')

    return None


def obtenerLocalizacionIP(ID):
    """ Get location of each router """

    try:
        # Get ROUTER ID of analysis
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        cursor.execute(""" SELECT ip_found
                           FROM   ip_found 
                           WHERE  analysis_id=%s;""" % ID)
        IP = cursor.fetchall()
        cursor.close()
        conexion.close()

    except Exception as e:
        print '\nERROR: %s' % str(e)

    else:
        # Connecting to MaxMind DB
        reader = geoip2_reader(direccion_geolite)
        datos = set()

        N = int(len(IP) / 1000) + 1
        n = 1

        # Getting an interface for each router ID
        for ip in IP:
            stdout.write('\r\t %s / %s' % ( int(n/1000), N) )
            stdout.flush()
            n = n + 1
            ip = ip[0]

            try:
                # Taking an IP to get its location
                response = reader.city(ip)
                
                try:
                    continent = str(response.continent.name)
                except UnicodeEncodeError:
                    continent = None

                try:
                    cod_continent = str(response.continent.code)
                except UnicodeEncodeError:
                    cod_continent = None

                try:
                    country = str(response.country.name)
                except UnicodeEncodeError:
                    country = None

                try:
                    cod_country = str(response.country.iso_code)
                except UnicodeEncodeError:
                    cod_country = None

                try:
                    region = str(response.subdivisions.most_specific.name)
                    region = region.translate(None, "'")
                except UnicodeEncodeError:
                    region = None

                try:
                    cod_region = str(response.subdivisions.most_specific.iso_code)
                except UnicodeEncodeError:
                    cod_region = None

                try:
                    city = str(response.city.name)
                    city = city.translate(None, "'")
                except UnicodeEncodeError:
                    city = None

                try:
                    latitude = float(response.location.latitude)
                except:
                    latitude = 0

                try:
                    longitude = float(response.location.longitude)
                except:
                    longitude = 0

                localizacion = (continent, cod_continent, country, cod_country, region, cod_region, city, latitude, longitude, ip)

                datos.add(localizacion)

            except Exception as e:
                pass
                
        reader.close()

        try:
            if len(datos) > 0:
                conexion = connect(conectar_BD())
                cursor = conexion.cursor()
                datos = list(datos)
                Q = set()
                for x in datos:
                    query = "UPDATE ip_found SET (continent, cod_continent, country, cod_country, region, cod_region, city, latitude, longitude) = ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s) WHERE ip_found='%s';\n" % x # WHERE analysis_id=%s AND router_id=%s;'
                    cursor.execute(query)
                    if (n % 1000) == 0: conexion.commit() 
                conexion.commit()
                cursor.close()
                conexion.close()

        except Exception as e:
            print str(e)
            pass

    finally:
        stdout.write('\r\tGetting location of IP: Done')
        stdout.flush()


    return None
