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

from os import system, remove
from os.path import exists, dirname, abspath
from sys import stdout
from time import sleep, time, asctime, gmtime
from psycopg2 import connect, Error as DatabaseError
from random import choice, randint
from socket import gethostbyname
from threading import Thread
from subprocess import PIPE, Popen, call
from netaddr import *
from getpass import getpass

from func_BD import conectar_BD, almacenar_mediciones_BD, almacenar_mediciones_B2B_BD
from func_admin import verNodos, confirmar

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

def elegirOrigen(api_server, auth, node_list):
    """ Generate a list of nodes to be the monitor in the exploration  """

    nodos_analisis = []
    nodos_no_utilizables = set()

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        cursor.execute('SELECT node FROM nodes_working;')
        aux = cursor.fetchall()
        for x in aux:
            nodos_no_utilizables.add(x[0])

        cursor.execute('SELECT node FROM blacklist_nodes_exploration;')
        aux = cursor.fetchall()
        for x in aux:
            nodos_no_utilizables.add(x[0])

        cursor.close()
        conexion.close()

    except DatabaseError as e:
        print 'Error: %s' % str(e)

    else:
        node_list = [nodo for nodo in node_list if nodo not in nodos_no_utilizables]

        if node_list:
            print '\nAvailable nodes: '
            try:
                verNodos(api_server, auth, node_list)
            except ErrorPlanetlab:
                raise

            aux = api_server.GetNodes (auth, {'hostname': node_list}, ['node_id'])

            ids = [x.get('node_id') for x in aux]
            nodos_analisis = []

            print color.UNDERLINE + '\nSelect nodes (ID):' + color.END
            print 'ALL: to select all available nodes'
            print '*:   to end selection\n'

            while True:
                nodo_origen = raw_input('Node:\t')

                if nodo_origen.strip().upper() == 'ALL':
                    nodos_analisis = node_list
                    break

                elif nodo_origen == '*':
                    break

                else:
                    if nodo_origen.isdigit():
                        nodo_origen = int(nodo_origen)
                        if nodo_origen in ids:
                            try:
                                nodos_analisis.append(api_server.GetNodes (auth, {'node_id': nodo_origen}, ['hostname']).pop(0).get('hostname'))
                            except ErrorPlanetlab:
                                raise
                        else:
                            print '\nNode not available.'
                    else:
                        print '\nEntry valid ID'

        else:
            print 'There is not available nodes'

    return nodos_analisis


def elegirDestino(api_server, auth, parametros):
    """ Generate a list of IP to be the target of the exploration """

    nodos_destino = []
    control = []
    nodo_origen = parametros.get('nodos_origen')
    parametros['mismos_destinos'] = 'S'
    cant_listas = len(nodo_origen)

    while True: # Selection the kind of target

        print color.UNDERLINE + '\n Select the kind of target:' + color.END
        print '1: Target: Planetlab \t Random   \t (only scamper)'
        print '2: Target: Planetlab \t Specific \t (only scamper)'
        print '3: Target: Internet  \t Random'
        print '4: Target: Internet  \t Specific'
        opcion = raw_input('\nOption: ')
        print '\n'

        if opcion == '1' and parametros.get('motivo') != 'B2B': # Planetlab - Random

            cant_max = api_server.GetSlices(auth, {}, ['max_nodes']).pop(0)
            cant_max = cant_max.get('max_nodes')

            while True:
                cantidad = raw_input('\nChoose the size of the target (0 to exit): ')
                if cantidad.isdigit():
                    cantidad = int(cantidad)
                    if cantidad >= 0 and cantidad <= cant_max:
                        break
                    else:
                        print '\nValue out of range: Min=1, Max=', cant_max
                else:
                    print '\nWrong'

            if cantidad:
                try:
                    print "\nGenerating list..."
                        # Nodos en mi origen
                    datos = api_server.GetNodes (auth, {'hostname': nodo_origen}, ['site_id'])
                        # Sitios origen
                    sitios_id_origen = [x.get('site_id') for x in datos]
                        # Todos los sitios menos los de origen
                    aux = api_server.GetSites (auth, {'~site_id': sitios_id_origen}, ['site_id', 'node_ids'])
                        # Elimino los sitios que no tienen nodos
                    sitios_disponibles = [x for x in aux if x.get('node_ids') != []]
                        # Sitios a elegir para seleccionar nodos
                    sitios_id = [x.get('site_id') for x in sitios_disponibles]
                        # Lista de nodos disponibles
                    nodos_disponibles = api_server.GetNodes (auth, {}, ['site_id','node_id', 'hostname'])
                except ErrorPlanetlab:
                    raise

                threads = list()
                print "\nChoosing destination nodes..."
                print '\nNodes selected:'
                for x in range(cantidad):
                    control.append(x)
                    t = Thread(target=elegir_destino_thread_opc1, args=(x, control, sitios_id, sitios_disponibles, nodos_disponibles, nodos_destino))
                    threads.append(t)
                    t.start()
                    sleep(0.5)

                while control:
                    sleep(1)

                aux = nodos_destino[:]
                nodos_destino = []
                [nodos_destino.append(aux) for x in range(cant_listas)]

            break

        elif opcion == '2' and parametros.get('motivo') != 'B2B': # Planetlab - Random

            nodos_id = []
            print 'Entry ID of desteny nodes (* to finish):'
            while True:
                ID = raw_input('ID:\t')
                if ID.isdigit():
                    nodos_id.append(int(ID))
                elif ID == '*':
                    break
                else:
                    print '\tID wrong'

            if nodos_id:
                try:
                    nodos_disponibles = api_server.GetNodes(auth, {'node_id': nodos_id}, ['hostname'])
                except ErrorPlanetlab:
                    raise

                threads = list()
                print 'Selected nodes are:'
                for x in nodos_disponibles:
                    x = x.get('hostname')
                    control.append(x)
                    t = Thread(target=elegir_destino_thread_opc2, args=(x, control, nodos_disponibles, nodos_destino))
                    threads.append(t)
                    t.start()

                while control:
                    sleep(0.5)

                aux = nodos_destino[:]
                nodos_destino = []
                [nodos_destino.append(aux) for x in range(cant_listas)]

            break

        elif opcion == '3': # Internet - Random

            # Max number of IP to make traceroute
			if parametros.get('motivo') == 'scamper':
                cant_max = 500 * 1000

				while True:
					cantidad = raw_input('\nSelect the number of destinations addresses (0 to finish): ')
					if cantidad.isdigit():
						cantidad = int(cantidad)
						if 0 < cantidad <= cant_max:
							break
						else:
							print '\nValue out of range: Min=1, Max=', cant_max
					else:
						print '\nWrong'

            elif parametros.get('motivo') == 'B2B':
                cant_max = 1
                cantidad = cant_max
            while True:
                opcion = raw_input('\nSame destinations in each monitor (y/n) [N]:\t')
                if len(opcion) == 0:
                    parametros['mismos_destinos'] = 'N'
                    break
                elif opcion.capitalize() == 'S':
                    parametros['mismos_destinos'] = 'Y'
                    break
                elif opcion.capitalize() == 'N':
                    parametros['mismos_destinos'] = 'N'
                    break
                else:
                    print 'Wrong'

            bloque = 176432

            try:
                conexion = connect(conectar_BD())
                cursor = conexion.cursor()

                cursor.execute('select network from address_block_ipv4 order by random() limit '+ str(bloque) +';')
                todas_las_redes = cursor.fetchall()

                if parametros.get('mismos_destinos') == 'N':

                    todos_los_destinos = set()

                    for i in range(cant_listas):

                        stdout.write("\r\tGenerating list of destinations..............%d%%" % int((i*100.0)/cant_listas))
                        stdout.flush()

                        lista_destinos = set()

                        j = 0

                        while len(lista_destinos) < cantidad:
                            try:
                                ip_destino = str(IPNetwork(todas_las_redes[j][0]).ip + randint(1,len(IPNetwork(todas_las_redes[j][0]))))

                                if ip_destino not in todos_los_destinos:
                                    lista_destinos.add(ip_destino)
                                    todos_los_destinos.add(ip_destino)

                                j += 1

                            except IndexError:
                                j = 0


                        nodos_destino.append(list(lista_destinos))

                    stdout.write("\r\tGenerating list of destinations..............%d%%" % 100)
                    stdout.flush()

                elif parametros.get('mismos_destinos') == 'S':

                    todos_los_destinos = set()

                    lista_destinos = set()

                    j = 0

                    while len(lista_destinos) <= cantidad:

                        ip_destino = str(IPNetwork(todas_las_redes[j][0]).ip + randint(1,len(IPNetwork(todas_las_redes[j][0]))))

                        if ip_destino not in todos_los_destinos:
                            lista_destinos.add(ip_destino)

                        j += 1

                        if j == bloque: j = 0

                    [nodos_destino.append(list(lista_destinos)) for x in range(cant_listas)]

            except DatabaseError:
                raise

            else:
                cursor.close()
                conexion.close()

            break

        elif opcion == '4': # Internet - Specific

            print 'Enter url or ip address destination (* to finish):'

            while True:
                sitio_web = raw_input('Site: ')
                if sitio_web == '*':
                    break
                else:
                    try:
                        dir_ip = gethostbyname(sitio_web)
                    except:
                        print 'Site not responsive'
                    else:
                        response = system("ping -c 1 -W 3 " + dir_ip + "  > /dev/null  2> /dev/null")
                        if response == 0:
                            nodos_destino.append(dir_ip)
                            print '\tSite added'
							if parametros.get('motivo') == 'B2B':
                                break
                        else:
                            print '\tSite not respond to ping'

            aux = nodos_destino[:]
            nodos_destino = []
            [nodos_destino.append(aux) for x in range(cant_listas)]

            break

        else:
            print 'Wrong option'

    return nodos_destino


def elegir_destino_thread_opc1(x, control, sitios_id, sitios_disponibles, nodos_disponibles, nodos_destino):
    """ Select a site and check if the site respond to a ping """

    while True:
        site = choice(sitios_id)
        sitios_id.remove(site)

        for z in sitios_disponibles:
            if site == z.get('site_id'):
                nodos = z.get('node_ids')
                nodo = choice(nodos)
                break

        asignacion = False
        for z in nodos_disponibles:
            if nodo == z.get('node_id'):
                hostname = z.get('hostname')
                asignacion = True
                break

        if asignacion == True:
            response = system("ping -c 1 -W 5 " + hostname + "  > /dev/null  2> /dev/null")
            if response == 0:
                nodos_destino.append(hostname)
                reg = '( ' + str(site) + ' )\t' + hostname
                print reg
                break
            else:
                sitios_id.append(site)

    control.remove(x)

    return None

def elegir_destino_thread_opc2(x, control, nodos_disponibles, nodos_destino):
    """ Select a node and check if the site respond to a ping """

    response = system("ping -c 1 -W 3 " + x + "  > /dev/null  2> /dev/null")
    if response == 0:
        print '\t' + x
        nodos_destino.append(x)
    else:
        print '\t' + x + ' (Do not respond to a ping)'
    control.remove(x)

    return None

def instalarAnalisis(parametros, script):
    """ Copy the exploration script the nodes in nodos_analisis """

    ID_analisis = '_%s' % parametros.get('analysis_id')

    slice_name = parametros.get('slice_name')
    nodos = parametros.get('nodos_origen')
    nodos_instalados = []

    archivo_direcciones = dirname(abspath(__file__)) + '/temp/direcciones_destino' + ID_analisis

    print '\nCreating exploration...\n'

    for n, nodo in enumerate(nodos):

        # Generate files of target
        if exists(archivo_direcciones):
            proceso = Popen(['rm %s' % archivo_direcciones], shell=True)
            proceso.wait()

        destinos = parametros.get('nodos_destino')[n]
        archivo = open(archivo_direcciones, 'w')
        archivo.write('0\n') # Reference time to calculate next initial TTL
        for x in destinos:
            archivo.write(x + ' 1\n')
        archivo.close()

        num_intentos = 3
        intento = 1
        instalacion = False

        while True:
            # Generate folder to group exploration files
            proceso = Popen(['parallel-ssh', '-H', nodo, '-l', slice_name, 'mkdir ~/%s' % parametros.get('usuario')], stdout=PIPE)
            salida = proceso.stdout.read()
            proceso.stdout.close()

            # Copy target file
            direccion_destino = '/home/%s/%s/' % (slice_name, parametros.get('usuario'))
            proceso = Popen(['parallel-scp','-H', nodo, '-l', slice_name, archivo_direcciones, direccion_destino], stdout=PIPE)
            salida = proceso.stdout.read()
            proceso.stdout.close()

            if '[SUCCESS]' in salida:
                # Generate the script which will make the exploration
                proceso = Popen(['parallel-ssh', '-H', nodo, '-l', slice_name, script], stdout=PIPE)
                salida = proceso.stdout.read()
                proceso.stdout.close()

                if '[SUCCESS]' in salida:
                    # Excecute the script
                    comando = 'ssh -f -l %s %s \"cd ~/%s/ ;nohup python analisis%s.py > /dev/null  2> /dev/null\"' % (slice_name, nodo, parametros.get('usuario'), ID_analisis)
                    if call([comando], shell=True) == 0:
                        instalacion = True
                        break
                    else:
                        break
                else:
                    break
            else:
                intento += 1
                if intento > num_intentos:
                    break
                else:
                    sleep(2)

        if instalacion:
            nodos_instalados.append(nodo)
            print '\t',(n+1),'/', (len(nodos)),', ', nodo, ':\t SUCCESS'
        else:
            print '\t',(n+1),'/', (len(nodos)),', ', nodo, ':\t ERROR'

        try:
            remove(archivo_direcciones)
        except:
            pass

    parametros['nodos_origen'] = nodos_instalados

    return None


def instalarAnalisis_B2B(parametros):
    """ Copy the exploration script the nodes in nodos_analisis to perform B2B analysis """

    slice_name = parametros.get('slice_name')
    nodos = parametros.get('nodos_origen')
    nodos_instalados = []
    script = dirname(abspath(__file__)) + '/pamplona_MySQL.py'

    IPdestino = parametros.get('nodos_destino')

    print '\nCreating exploration...\n'
    for n, nodo in enumerate(nodos):
        num_intentos = 3
        intento = 1
        instalacion = False

        while True:
            # 1. Transferir script
            carpetaDestino = '/home/%s/pamplona/' % (slice_name)
            proceso = Popen(['parallel-scp','-H', nodo, '-l', slice_name, script, carpetaDestino], stdout=PIPE)
            salida = proceso.stdout.read()
            proceso.stdout.close()

            if '[SUCCESS]' in salida:
                # 2. Ejecutar scrip con los parametros de entrada correspondientes
                comando = 'ssh -f -l %s %s \"cd ~/pamplona/ ;nohup sudo python pamplona_MySQL.py %s %s %s %s > /dev/null  2> /dev/null\"' % (slice_name, nodo, parametros.get('n_hops'), IPdestino[n][0], parametros.get('duracion_traceroutes'), slice_name)
                if call([comando], shell=True) == 0:
                    instalacion = True
                    break
            else:
                    break

            else:
                intento += 1
                if intento > num_intentos:
                    break
                else:
                    sleep(2)

        if instalacion:
            nodos_instalados.append(nodo)
            print '\t',(n+1),'/', (len(nodos)),', ', nodo, ':\t SUCCESS'
        else:
            print '\t',(n+1),'/', (len(nodos)),', ', nodo, ':\t ERROR'

        try:
            remove(archivo_direcciones)
        except:
            pass

    parametros['nodos_origen'] = nodos_instalados

    return None
	
	
def generar_lista_ip(nodos_destino):
    """ Generate a list of IP given a list of hostname  """

    lista_ip = []

    for x in nodos_destino:
        try:
            ip = str(gethostbyname(x))
        except:
            ip = 'x' # To indicate that traceroute dont start
        else:
            lista_ip.append(ip)

    return lista_ip


def resumenConf(parametros):
    """ Brief summary of the exploration setting """

    print color.UNDERLINE + '\nSummary:' + color.END
    print 'ID exploration:\t\t', parametros.get('analysis_id')
    print 'Description:\t\t', parametros.get('descripcion')
    if parametros.get('trace_type') != 'B2B': print 'Period:\t\t\t', parametros.get('periodo_traceroutes'),'ss'
    print 'Duration:\t\t', str(int(parametros.get('duracion_traceroutes'))/3600), 'hh'
    print '# Monitors:\t\t', len(parametros.get('nodos_origen'))
    print '# Target:\t\t', len(parametros.get('nodos_destino')[0])
    if parametros.get('trace_type') != 'B2B': print '# Ping:\t\t\t', parametros.get('ping_ejecutar')

    return None


def mostrarRegistro(accion, opcion, motivo = 'scamper'):
    """ Brief summary of the previous explorations """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        if accion == 'almacenamiento' and motivo == 'scamper':
            cursor.execute('SELECT analysis_id, usr, description, TS, TS_epoch, duration, stored, topology_state FROM analysis where stored=\'' + str(opcion).lower() + '\' and trace_type<>\'B2B\' order by analysis_id;')
        elif accion == 'almacenamiento' and motivo == 'B2B':
            cursor.execute('SELECT analysis_id, usr, description, TS, TS_epoch, duration, stored, topology_state FROM analysis where stored=\'' + str(opcion).lower() + '\' and trace_type=\'B2B\' order by analysis_id;')
        elif accion == 'resolucion_topologia':
            cursor.execute('SELECT analysis_id, usr, description, TS, TS_epoch, duration, stored, topology_state FROM analysis where topology_state=\'' + str(opcion).lower() + '\' order by analysis_id;')

        else:
           print 'Error'

        reg = cursor.fetchall()

        reg = reg[:]

        cursor.close()
        conexion.close()

        if reg:
            salida = True
            for x in reg:
                print 'ID:\t\t', x[0], '\t(', x[1], ')'
                print 'Date:\t\t', x[3], 'UTC (', tiempoPendiente(x[0]), ')'
                print 'Description:\t', x[2]
                print 'Stored:\t\t', x[6].upper()
                print 'Topology:\t', x[7].upper()
                print '\n'
        else:
            salida = False

    except Exception as e:
        print 'Error: %s' % str(e)

    return salida


def tiempoPendiente(ID):
    """ Calcula el tiempo que falta para que un analisis termine, y si ya finalizó devuelve 'Finalizado' """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        ID = str(ID)

        cursor.execute('SELECT TS_epoch, duration, ping_type, period, gap_limit, pps, num_dest, trace_type FROM analysis where analysis_id='+ID+';')

        x = cursor.fetchall()[0]

        inicio = int(x[0])
        duracion = int(x[1])
        ping = x[2]
        trace_type = x[7].upper()

        if trace_type == 'B2B':
            t_falta = (inicio + duracion) - int(time())
            
        else:
            if ping != None: # If round of ping is selected then sum an hour
                ronda_ping = 3600
            else:
                ronda_ping = 0
    
            periodo_ingresado = int(x[3])
    
            gap_limit = int(x[4])
            pps = int(x[5])
            cant_destinos = int(x[6])
    
            paquetes_por_trace = 8                          # Estimador
            porcentaje_destinos_incompletos = 0.5  * 1.0    # Estimador
    
            cantidad_media_paquetes_por_trace = (gap_limit * porcentaje_destinos_incompletos) + paquetes_por_trace
    
            cantidad_paquetes_total = cantidad_media_paquetes_por_trace * cant_destinos
    
            periodo_estimado = int(cantidad_paquetes_total / pps)
    
            periodo_trace = max(periodo_ingresado, periodo_estimado)
    
            t_falta = (inicio + duracion + periodo_trace + ronda_ping) - int(time())
    
        if t_falta > 0:
            d = int(t_falta/86400)
            h = int((t_falta-(d*86400))/3600)
            m = int((t_falta-(d*86400)-(h*3600))/60)
            s = t_falta-((d*86400)+(h*3600)+(m*60))
            tiempo = str(d) + 'd - ' + str(h) + ':' + str(m) + ':' + str(s)
        else:
            tiempo = 'Finalizado'

    except:
        pass

    finally:
        cursor.close()
        conexion.close()

    return tiempo


def almacenarMediciones(auth, slice_name, node_list, ID):
    """ Store in DB the results of an exploration """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        # Load data of exploration to process
        cursor.execute('SELECT analysis_id, TS_epoch, duration, nodes, stored, nodes_stored, trace_type FROM analysis where analysis_id=%s;' % ID)
        x = cursor.fetchall()[0]

        trace_type = x[6].upper()
        if x[4] == 'n' and tiempoPendiente(str(ID)) == 'Finalizado':

            nodos_analisis = x[3].split('|')
            try:
                nodos_ya_almacenados = x[5].split('|')
            except:
                nodos_ya_almacenados = []
            nodos_almacenar = [nodo for nodo in nodos_analisis if (nodo not in nodos_ya_almacenados)]

            try:
                if trace_type != 'B2B':
                    if almacenar_mediciones_BD(auth, slice_name, str(ID), nodos_almacenar, ingresarPassSudo()):
                        print 'The results of all nodes have been stored \n'
                    else:
                        print 'There have been pending results\n'
                else:
                    if almacenar_mediciones_B2B_BD(auth, slice_name, str(ID), nodos_almacenar):
                        print 'The results of all nodes have been stored \n'
                    else:
                        print 'There have been pending results\n'
                        
                cursor.execute('SELECT nodes, nodes_stored FROM analysis WHERE analysis_id=' + str(ID) + ';')
                aux = cursor.fetchall()[0]
                try:
                    nodos_experimento = aux[0].split('|')
                except:
                    nodos_experimento = ''

                try:
                    nodos_almacenados = aux[1].split('|')
                except:
                    nodos_almacenados = ''

                aux = [x for x in nodos_experimento if (x in node_list) and (x not in nodos_almacenados)]

                if aux:
                    while True:
                        opcion = raw_input(str(len(aux)) + ' nodes without stored its results. do you want to discard their data? (y/n):\t')
                        if opcion.upper() == 'Y':
                            # Seteo el analisis como ya almacenado y elimino los nodos de la tabla Nodes_working
                            cursor.execute('UPDATE analysis SET stored=\'y\' WHERE analysis_id=' + str(ID) + ';')
                            cursor.execute('DELETE FROM nodes_working WHERE analysis_id=' + str(ID) + ';')
                            conexion.commit()

                            # Remove data in nodes
                            if trace_type != 'B2B':                                

                                for nodo in aux:
                                    command = 'parallel-ssh -H ' + str(nodo) + ' -l ' + slice_name + ' ' + 'sudo rm /home/'+slice_name+'/'+auth.get('Username')+'/*_'+str(ID)+'*'
                                    proceso = Popen([command], shell=True, stdout=PIPE)
                                    basura = proceso.stdout.readlines()
                                    proceso.stdout.close()
                            else:
                                for nodo in aux:
                                    command = 'parallel-ssh -H ' + str(nodo) + ' -l ' + slice_name + ' ' + 'sudo rm /home/'+slice_name+'/pamplona/resultados.tar.gz'
                                    proceso = Popen([command], shell=True, stdout=PIPE)
                                    basura = proceso.stdout.readlines()
                                    proceso.stdout.close()


                            break
                        elif opcion.upper() == 'N':
                            break
                        else:
                            print 'Wrong'
                else:
                    if trace_type != 'B2B':
                        cursor.execute('UPDATE analysis SET stored=\'y\', topology_state=\'0\' WHERE analysis_id=' + str(ID) + ';')
                    else:
                        cursor.execute('UPDATE analysis SET stored=\'y\', topology_state=\'9\' WHERE analysis_id=' + str(ID) + ';')
                    conexion.commit()

            except Exception as e:
                print 'Error: %s' % str(e)
                pass

        else:
            print 'It is not possible to store exploration'

        cursor.close()
        conexion.close()

    except DatabaseError as e:
        print 'Error: %s' % str(e)

    except Exception as e:
        print 'ERROR:\n %s' % str(e)


    return None


def obtenerParametros_B2B(parametros):
    """ Solicito los parametros restantes para ejecutar los exploracion B2B """

    # Number of packet to use in the B2B traceroute
    try:
        while True:
            opcion = raw_input('\npackets [20]: \t')
            if len(opcion) == 0:
                parametros['n_hops'] = 20
                break
            elif 1 <= int(opcion) <= 168:
                parametros['n_hops'] = int(opcion)
                break
            else:
                print 'Wrong: 1 <= duration <= 168'
    except:
        print 'Wrong'


    # Period of traceroutes
    parametros['periodo_traceroutes'] = None

    # Duration
    #tiempo = 1 * 3600 * 24
    #parametros['duracion_traceroutes'] = str(tiempo)

    try:
        while True:
            opcion = raw_input('Duration in hours [24]: \t')
            if len(opcion) == 0:
                parametros['duracion_traceroutes'] = 24 * 3600
                break
            elif 1 <= int(opcion) <= 168:
                parametros['duracion_traceroutes'] = int(opcion) * 3600
                break
            else:
                print 'Wrong: 1 <= duration <= 168'
    except:
        print 'Wrong'

    # Probe type
    parametros['trace_type'] = 'B2B'

    # PPS
    parametros['pps'] = None

    # GAPLIMIT
    parametros['gaplimit'] = None

    # WAIT
    parametros['wait'] = None

    ### Recalculation of initial TTL
    parametros['recalcular_TTL'] = None
    parametros['tiempo_recalculo_TTL'] = None

    ### Ping
    parametros['ping_ejecutar'] = None
    parametros['ping_type'] = None
    parametros['ping_ttl'] = None
    parametros['ping_sent'] = None

    return None  

	
def obtenerParametros(parametros):
    """ Solicito los parametros restantes para ejecutar los traceroutes """

    # Parameters
    parametros['source_port'] = 36394
    parametros['dest_port'] = 33435
    parametros['max_loops'] = 1            # specifies the action to take when a loop is encountered. A value of 1 tells scamper to probe beyond the first loop in the trace."
    parametros['loop_action'] = 1          # Maximo numero de loops detectados antes de detener el traceroute ('0' desactiva la deteccion de loops)

    # Period of traceroutes
    periodo_minimo = '30'
    while True:
        periodo = raw_input('\nPeriod of traceroutes (sec):\t')
        try:
            if int(periodo) >= (int(periodo_minimo) - 1):
                break
            else:
                print 'The minimum period is: ', periodo_minimo, 'sec'
        except:
            print 'Wrong'

    parametros['periodo_traceroutes'] = periodo

    # Duration
    print '\nDuration of exploration'
    print '[-h hours] [-d days] [-w weeks]\n'

    while True:
        duracion  = raw_input('Ingresar: ')
        duracion = duracion.split()

        if len(duracion) == 2 and duracion[1].isdigit():
            if int(duracion[1]) > 0:
                if '-h' in duracion:
                    tiempo = int(duracion[1]) * 3600
                    break
                elif '-d' in duracion:
                    tiempo = int(duracion[1]) * 3600 * 24
                    break
                elif '-w' in duracion:
                    tiempo =  int(duracion[1]) * 3600 * 24 * 7
                    break
                else:
                    print 'Wrong\n'
            else:
                print 'The value must be an integer greater than zero\n'
        else:
            print 'Wrong\n'

    parametros['duracion_traceroutes'] = str(tiempo)

    # Probe type
    while True:
        opcion = raw_input('\nProbe type [UDP-Paris]: \t')
        if len(opcion) == 0:
            parametros['trace_type'] = 'udp-paris'
            break
        elif opcion.lower() in ('icmp-paris', 'udp-paris', 'udp', 'icmp', 'tcp', 'tcp-ack'):
            parametros['trace_type'] = opcion.lower()
            break
        else:
            print 'Wrong'

    # PPS
    try:
        while True:
            opcion = raw_input('\nPPS [20]: \t')
            if len(opcion) == 0:
                parametros['pps'] = 20
                break
            elif 20 <= int(opcion) <= 1000:
                parametros['pps'] = int(opcion)
                break
            else:
                print 'Wrong: 20 <= pps <= 1000'
    except:
        print 'Wrong'

    # GAPLIMIT
    try:
        while True:
            opcion = raw_input('\nGAPLIMIT [5]: \t')
            if len(opcion) == 0:
                parametros['gaplimit'] = 5
                break
            elif 1 <= int(opcion) <= 8:
                parametros['gaplimit'] = int(opcion)
                break
            else:
                print 'Wrong: 1 <= GAPLIMIT <= 8'
    except:
        print 'Wrong'

    # WAIT
    try:
        while True:
            opcion = raw_input('\nWAIT [5]: \t')
            if len(opcion) == 0:
                parametros['wait'] = 3
                break
            elif 1 <= int(opcion) <= 10:
                parametros['wait'] = int(opcion)
                break
            else:
                print 'Wrong: 1 <= WAIT <= 10'
    except:
        print 'Wrong'

    ### Recalculation of initial TTl
    while True:
        opcion = raw_input('\nAmount time to apply for the recalculation of initial TTL, in hours, [0 for only time], -1 to unable:\t')
        if len(opcion) == 0: # Default value
            parametros['recalcular_TTL'] = 'S'
            parametros['tiempo_recalculo_TTL'] = parametros.get('duracion_traceroutes')
            break
        elif int(opcion) == -1:
            parametros['recalcular_TTL'] = 'N'
            parametros['tiempo_recalculo_TTL'] = 0
            break
        elif int(opcion) == 0:
            parametros['recalcular_TTL'] = 'S'
            parametros['tiempo_recalculo_TTL'] = parametros.get('duracion_traceroutes')
            break
        elif 1 <= int(opcion) <= 12:
            parametros['recalcular_TTL'] = 'S'
            parametros['tiempo_recalculo_TTL'] = int(opcion) * 3600
            break
        else:
            print 'Wrong'

    ### Ping
    while True:
        opcion = raw_input('\nSeries of ping [N] \t')
        if len(opcion) == 0 or opcion.capitalize() == 'N':
            parametros['ping_ejecutar'] = 'N'
            parametros['ping_type'] = None
            parametros['ping_ttl'] = 64
            parametros['ping_sent'] = 0
            break
        elif opcion.capitalize() == 'Y':
            parametros['ping_ejecutar'] = 'Y'
            parametros['ping_type'] = 'icmp-echo'
            parametros['ping_ttl'] = 64
            while True:
                opcion = raw_input('Number of ping for each IP [1]: \t')
                if len(opcion) == 0:
                    parametros['ping_sent'] = 1
                    break
                elif 1 <= int(opcion) <= 10:
                    parametros['ping_sent'] = int(opcion)
                    break
                else:
                    print 'Wrong: 1 <= #PING <= 10'
            break
        else:
            print 'Wrong'

    return None


def ingresarPassSudo():
    """ Request to enter the sudo pass """

    while True:
        sudoPassword = getpass('Enter sudo pass: ')
        confirmar_pass = getpass('Confirm sudo pass: ')
        if sudoPassword == confirmar_pass and len(sudoPassword)>0:
            break
        else:
            print '\nWrong Pass. Retry.\n'

    p = system('echo %s | sudo -S %s   >/dev/null 2>&1' % (sudoPassword, 'sleep 0.01'))

    return sudoPassword


def cancelarAnalisis(user, slice_name, node_list, ID):
    """ Delete a running/waiting exploration """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        cursor.execute('SELECT analysis_id, nodes, nodes_stored, trace_type FROM analysis WHERE analysis_id='+str(ID)+' AND stored=\'n\';')
        x = cursor.fetchall()[0]

        trace_type = x[3].upper()

        if trace_type != 'B2B':
            try:
                analysis_id = x[0]
                nodos_analisis = x[1].split('|')
                try:
                    nodos_ya_almacenados = x[2].split('|')
                except:
                    nodos_ya_almacenados = []
    
                nodos_eliminar = [nodo for nodo in nodos_analisis if (nodo in node_list) and (nodo not in nodos_ya_almacenados)]
    
            except DatabaseError:
                print '\nWrong ID\n'
    
            except IndexError:
                print '\nWrong ID\n'
    
            else:
                if confirmar('Cancel exploration'):
                    cursor.execute('DELETE FROM analysis CASCADE WHERE analysis_id=' + str(ID) + ';')
                    cursor.execute('DELETE FROM nodes_working WHERE analysis_id=' + str(ID) + ';')
                    lista_nodos = '"' + ' '.join(nodos_eliminar) + '"'
                    command = 'parallel-ssh -H ' + lista_nodos + ' -l ' + slice_name + ' ' + 'sudo rm /home/'+slice_name+'/'+user+'/*_'+str(ID)+'*'
                    proceso = Popen([command], shell=True, stdout=PIPE)
                    salida = proceso.stdout.read()
                    proceso.stdout.close()
                    print '\n',salida
    
                    conexion.commit()
        else:
            print 'It is not possible cancel a traceroutes B2B exploration'

        cursor.close()
        conexion.close()

    except DatabaseError as e:
        print 'Error: %s' % str(e)


    return None

def eliminarResultados(ID, opcion):
    """ Remove dato of exploration <ID> from DB """

    if confirmar('Remove data'):
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        if opcion == 'analysis':
            cursor.execute('DELETE FROM analysis WHERE analysis_id=%s' % ID)

        elif opcion == 'alias_resolution':
            cursor.execute('UPDATE analysis SET topology_state=\'0\' WHERE analysis_id=%s' % ID)
            cursor.execute('DELETE FROM alias_resolution WHERE analysis_id=%s' % ID)
            cursor.execute('DELETE FROM ip_metodo_midar WHERE analysis_id=%s' % ID)
            cursor.execute("DELETE FROM ip_resolution WHERE analysis_id=%s" % ID)
            cursor.execute('DELETE FROM nodes_working WHERE analysis_id=%s' % ID)
            cursor.execute('DELETE FROM interfaces WHERE analysis_id=%s' % ID)
            cursor.execute('DELETE FROM routers WHERE analysis_id=%s' % ID)

        conexion.commit()
        cursor.close()
        conexion.close()

    return None


def configuracionAnalisis(parametros):
    """ Generate the exploration script """

    # Parameters of execution
    nodos_por_hilo = 1500
    tamanio_maximo = 1024 # MB

    # Key parameters assigned
    ID_analisis = '_%s' % parametros.get('analysis_id')

    pps = parametros.get('pps')
    wait = parametros.get('wait')
    trace_type = parametros.get('trace_type')
    source_port = parametros.get('source_port')
    dest_port = parametros.get('dest_port')
    recalcular_TTL = parametros.get('recalcular_TTL')
    tiempo_recalculo_TTL = parametros.get('tiempo_recalculo_TTL')
    periodo = parametros.get('periodo_traceroutes')
    duracion = parametros.get('duracion_traceroutes')
    slice_name = parametros.get('slice_name')
    gaplimit = parametros.get('gaplimit')
    max_loops = parametros.get('max_loops')
    loop_action = parametros.get('loop_action')

    # File name
    analisis_script = '~/%s/analisis%s.py' % (parametros.get('usuario'), ID_analisis)
    resultados = '~/%s/resultados%s.warts' % (parametros.get('usuario'), ID_analisis)
    dir_destino = 'direcciones_destino%s' % ID_analisis
    temp = 'temp%s.warts' % ID_analisis
    lista_ping = 'lista_ping%s' % ID_analisis
    temp_ping = 'temp_ping%s' % ID_analisis

    # Start script
    lista_comandos = []

    lista_comandos.append('sudo mkdir %s *' % parametros.get('usuario'))
    lista_comandos.append('sudo /usr/local/bin/scamper -O planetlab -O warts -o %s -i 127.0.0.1' % resultados)
    lista_comandos.append('sudo chown %s *' % slice_name)

    ### script python which execute the exploration ###
    lista_comandos.append('echo "# -*- coding: utf-8 -*-" > ' + analisis_script)
    lista_comandos.append('echo " " >> ' + analisis_script)
    lista_comandos.append('echo "from subprocess import call, PIPE, Popen" >> ' + analisis_script)
    lista_comandos.append('echo "from time import time, sleep, asctime, gmtime" >> ' + analisis_script)
    lista_comandos.append('echo "from threading import Thread" >> ' + analisis_script)
    lista_comandos.append('echo "from os import remove, path" >> ' + analisis_script)
    lista_comandos.append('echo "import logging" >> ' + analisis_script)
    lista_comandos.append('echo " " >> ' + analisis_script)
    ### Calculation TTL_inicial
    lista_comandos.append('echo "def calculo_TTL_inicial(trace, hops_alcanzados): " >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    TTL_calculado = 1" >> ' + analisis_script)
    lista_comandos.append('echo "    for hop in trace:" >> ' + analisis_script)
    lista_comandos.append('echo "        if hop in hops_alcanzados:" >> ' + analisis_script)
    lista_comandos.append('echo "            TTL_calculado += 1" >> ' + analisis_script)
    lista_comandos.append('echo "        else:" >> ' + analisis_script)
    lista_comandos.append('echo "            break" >> ' + analisis_script)
    lista_comandos.append('echo "    return str(TTL_calculado)" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    ### Recalculation TTL_inicial
    lista_comandos.append('echo "def ejecutar_recalculo_TTL(ronda): " >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    referencia_1 = int(time())" >> ' + analisis_script)
    lista_comandos.append('echo "    proceso = Popen([\'/usr/local/bin/sc_analysis_dump\',\'-ClctrHis\', \'' + '0' + temp+ '\'], stdout=PIPE)" >> ' + analisis_script)
    lista_comandos.append('echo "    salida = proceso.stdout.read()" >> ' + analisis_script)
    lista_comandos.append('echo "    proceso.stdout.close()" >> ' + analisis_script)
    lista_comandos.append('echo "    salida = salida.splitlines()" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    referencia_2 = int(time())" >> ' + analisis_script)
    lista_comandos.append('echo "    archivo = open(\\\"'+dir_destino+'\\\", \\\"w\\\")" >> ' + analisis_script)
    lista_comandos.append('echo "    archivo.write(str(int(time())) + \\\"\\\\n\\\")" >> ' + analisis_script)
    lista_comandos.append('echo "    hops_alcanzados = set()" >> ' + analisis_script)
    lista_comandos.append('echo "    for x in salida:" >> ' + analisis_script)
    lista_comandos.append('echo "        x = x.split()" >> ' + analisis_script)
    lista_comandos.append('echo "        archivo.write(x[1] + \' \' + str(calculo_TTL_inicial(x[3:], hops_alcanzados)) + \\\"\\\\n\\\")" >> ' + analisis_script)
    lista_comandos.append('echo "        hops_alcanzados.update(x[3:])" >> ' + analisis_script)
    lista_comandos.append('echo "    archivo.close()" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    mensaje = \'RECAL - RONDA: \' + ronda + \' - D1: \' + str(referencia_2 - referencia_1) + \' - D2: \' + str(int(time()) - referencia_2)" >> ' + analisis_script)
    lista_comandos.append('echo "    logging.info(mensaje)" >> ' + analisis_script)
    lista_comandos.append('echo "     " >> ' + analisis_script)
    lista_comandos.append('echo "    return None" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    ### Scamper Threads
    lista_comandos.append('echo "def thread_scamper(hilo, control, cant_destinos, destino_y_TTL, recalcular):" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    pps = max(5,int(' + str(pps) + ' * ((len(destino_y_TTL)*1.0)/cant_destinos)))" >> ' + analisis_script)
    lista_comandos.append('echo "    if recalcular:" >> ' + analisis_script)
    lista_comandos.append('echo "        pps = min(1000, int(1.5 * pps))" >> ' + analisis_script)
    lista_comandos.append('echo "    else:" >> ' + analisis_script)
    lista_comandos.append('echo "        if hilo == \'0\':" >> ' + analisis_script)
    lista_comandos.append('echo "            pps = min(1000, (max(80, pps * 3)))" >> ' + analisis_script)
    lista_comandos.append('echo "    archivo_temp = str(hilo) + \\\"' + temp + '\\\"" >> ' + analisis_script)
    lista_comandos.append('echo "    comando = \'sudo /usr/local/bin/scamper -O planetlab -O warts -o \' + archivo_temp + \' -p \' + str(pps) + \' -I \'" >> ' + analisis_script)
    lista_comandos.append('echo "    sport = str(36394 + int(hilo))" >> ' + analisis_script)
    lista_comandos.append('echo "    dport = str(33435 + int(hilo))" >> ' + analisis_script)
    lista_comandos.append('echo "    for x in destino_y_TTL:" >> ' + analisis_script)
    lista_comandos.append('echo "        comando += \' \\\"trace -q 1 -s \' + sport + \' -d \' + dport + \' -g ' + str(gaplimit) + ' -l ' + str(max_loops) + ' -L ' + str(loop_action) + ' -w ' + str(wait) + ' -P ' + trace_type + ' -f \' + x[1] + \' \' + x[0] + \'\\\" \' " >> ' + analisis_script)
    lista_comandos.append('echo "    call([comando], shell=True)" >> ' + analisis_script)
    lista_comandos.append('echo "    control.pop()" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    return None" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    ### Traceroutes
    lista_comandos.append('echo "def ejecutar_traceroutes(ronda): " >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    logging.basicConfig(filename=\'analisis'+ID_analisis+'.log\',level=logging.INFO, format=\'%(asctime)s, %(funcName)s, %(levelname)s, %(message)s\',  datefmt=\'%Y/%m/%d %H:%M:%S\')" >> ' + analisis_script)
    lista_comandos.append('echo "    ID = \''+ID_analisis+'\'" >> ' + analisis_script)
    lista_comandos.append('echo "    destinos = []" >> ' + analisis_script)
    lista_comandos.append('echo "    TTL_inicial = []" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    try:" >> ' + analisis_script) ### Read file containing the destination addresses
    lista_comandos.append('echo "        archivo = open(\\\"'+dir_destino+'\\\", \\\"r\\\")" >> ' + analisis_script)
    lista_comandos.append('echo "        aux = archivo.readlines()" >> ' + analisis_script)
    lista_comandos.append('echo "        archivo.close()" >> ' + analisis_script)
    lista_comandos.append('echo "    except:" >> ' + analisis_script)
    lista_comandos.append('echo "        logging.error(\'TRACE - No se ha encontrado archivo de direcciones\')" >> ' + analisis_script)
    lista_comandos.append('echo "    else:" >> ' + analisis_script)
    lista_comandos.append('echo "        tiempo_ultimo_recalculo = int(aux[0].split()[0])" >> ' + analisis_script)
    lista_comandos.append('echo "        " >> ' + analisis_script)
    lista_comandos.append('echo "        destinos = {}" >> ' + analisis_script)
    lista_comandos.append('echo "        for x in aux[1:]:" >> ' + analisis_script)
    lista_comandos.append('echo "            x = x.split()" >> ' + analisis_script)
    lista_comandos.append('echo "            destinos[x[0]] = x[1]" >> ' + analisis_script) # Destination - initial TTL
    lista_comandos.append('echo "        " >> ' + analisis_script)
    lista_comandos.append('echo "        if \'' + str(recalcular_TTL) + '\' == \'S\':" >> ' + analisis_script) ### Create vector for TTL_inicial
    lista_comandos.append('echo "            if (int(time()) - tiempo_ultimo_recalculo) > ' + str(tiempo_recalculo_TTL) + ':" >> ' + analisis_script)
    lista_comandos.append('echo "                recalcular = True" >> ' + analisis_script)
    lista_comandos.append('echo "                keys = destinos.keys()" >> ' + analisis_script)
    lista_comandos.append('echo "                for k in keys:" >> ' + analisis_script)
    lista_comandos.append('echo "                    destinos[k] = \'1\'" >> ' + analisis_script)
    lista_comandos.append('echo "            else:" >> ' + analisis_script)
    lista_comandos.append('echo "                recalcular = False" >> ' + analisis_script)
    lista_comandos.append('echo "        else:" >> ' + analisis_script)
    lista_comandos.append('echo "            recalcular = False" >> ' + analisis_script)
    lista_comandos.append('echo "        " >> ' + analisis_script)
    lista_comandos.append('echo "        cant_destinos = len(destinos)" >> ' + analisis_script) ### Excecute the traceroutes
    lista_comandos.append('echo "        cant_hilos = (cant_destinos/'+str(nodos_por_hilo)+')" >> ' + analisis_script)
    lista_comandos.append('echo "        if (len(destinos)%'+str(nodos_por_hilo)+')>0:" >> ' + analisis_script)
    lista_comandos.append('echo "            cant_hilos += 1" >> ' + analisis_script)
    lista_comandos.append('echo "        control = range(cant_hilos)" >> ' + analisis_script)
    lista_comandos.append('echo "        " >> ' + analisis_script)
    lista_comandos.append('echo "        items_todos = destinos.items()" >> ' + analisis_script)
    lista_comandos.append('echo "        " >> ' + analisis_script)
    lista_comandos.append('echo "        threads = list()" >> ' + analisis_script)
    lista_comandos.append('echo "        referencia = int(time())" >> ' + analisis_script)
    lista_comandos.append('echo "        for hilo in range(cant_hilos):" >> ' + analisis_script)
    lista_comandos.append('echo "            items = []" >> ' + analisis_script)
    lista_comandos.append('echo "            try:" >> ' + analisis_script)
    lista_comandos.append('echo "                for y in range('+str(nodos_por_hilo)+'):" >> ' + analisis_script)
    lista_comandos.append('echo "                    items.append(items_todos.pop())" >> ' + analisis_script)
    lista_comandos.append('echo "            except:" >> ' + analisis_script)
    lista_comandos.append('echo "                pass" >> ' + analisis_script)
    lista_comandos.append('echo "            t = Thread(target=thread_scamper, args=(hilo, control, cant_destinos, items, recalcular))" >> ' + analisis_script)
    lista_comandos.append('echo "            threads.append(t)" >> ' + analisis_script)
    lista_comandos.append('echo "            t.start()" >> ' + analisis_script)
    lista_comandos.append('echo "        " >> ' + analisis_script)
    lista_comandos.append('echo "        while control:" >> ' + analisis_script)
    lista_comandos.append('echo "            sleep(1)" >> ' + analisis_script)
    lista_comandos.append('echo "        " >> ' + analisis_script)
    lista_comandos.append('echo "        mensaje = \'TRACE - RONDA: \' + ronda + \' - DURACION: \' + str(int(time()) - referencia)" >> ' + analisis_script)
    lista_comandos.append('echo "        logging.info(mensaje)" >> ' + analisis_script)
    lista_comandos.append('echo "        " >> ' + analisis_script)
    lista_comandos.append('echo "        for hilo in range(cant_hilos):" >> ' + analisis_script) ### group all file results
    lista_comandos.append('echo "            archivo_temp = str(hilo) + \\\"' + temp + '\\\"" >> ' + analisis_script)
    lista_comandos.append('echo "            comando = \'sudo /usr/local/bin/sc_wartscat -o ' + resultados + ' \' + archivo_temp " >> ' + analisis_script)
    lista_comandos.append('echo "            call([comando], shell=True)" >> ' + analisis_script)
    lista_comandos.append('echo "            if hilo>0: " >> ' + analisis_script)
    lista_comandos.append('echo "                comando = \'sudo /usr/local/bin/sc_wartscat -o ' + '0' + temp + ' \' + archivo_temp " >> ' + analisis_script)
    lista_comandos.append('echo "                call([comando], shell=True)" >> ' + analisis_script)
    lista_comandos.append('echo "                remove(archivo_temp)" >> ' + analisis_script)
    lista_comandos.append('echo "            " >> ' + analisis_script)
    lista_comandos.append('echo "    return recalcular" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    ### Generate list of found IP to make them the pins
    lista_comandos.append('echo "def generar_lista_ping():" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    logging.basicConfig(filename=\'analisis'+ID_analisis+'.log\',level=logging.INFO, format=\'%(asctime)s, %(funcName)s, %(levelname)s, %(message)s\',  datefmt=\'%Y/%m/%d %H:%M:%S\')" >> ' + analisis_script)
    lista_comandos.append('echo "    referencia = int(time())" >> ' + analisis_script)
    lista_comandos.append('echo "    lista_ip = set()" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    proceso = Popen([\'/usr/local/bin/sc_analysis_dump\',\'-ClctrHis\', \'' + '0' + temp+ '\'], stdout=PIPE)" >> ' + analisis_script)
    lista_comandos.append('echo "    salida = proceso.stdout.read()" >> ' + analisis_script)
    lista_comandos.append('echo "    proceso.stdout.close()" >> ' + analisis_script)
    lista_comandos.append('echo "    salida = salida.splitlines()" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    for trace in salida:" >> ' + analisis_script)
    lista_comandos.append('echo "        trace = trace.split()" >> ' + analisis_script)
    lista_comandos.append('echo "        [lista_ip.add(hop.split(\';\')[0]) for hop in trace[3:]]" >> ' + analisis_script)
    lista_comandos.append('echo "    lista_ip.discard(\'q\')" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    archivo = open(\\\"'+lista_ping+'\\\", \\\"w\\\")" >> ' + analisis_script)
    lista_comandos.append('echo "    for x in lista_ip:" >> ' + analisis_script)
    lista_comandos.append('echo "        archivo.write(x + \\\"\\n\\\")" >> ' + analisis_script)
    lista_comandos.append('echo "    archivo.close()" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    mensaje = \'PING - BUSQUEDA - IPs: \' + str(len(lista_ip)) + \' - DURACION: \' + str(int(time()) - referencia)" >> ' + analisis_script)
    lista_comandos.append('echo "    logging.info(mensaje)" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    return None" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    ### Excecution of ping
    lista_comandos.append('echo "def ejecutar_ronda_ping():" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    logging.basicConfig(filename=\'analisis'+ID_analisis+'.log\',level=logging.INFO, format=\'%(asctime)s, %(funcName)s, %(levelname)s, %(message)s\',  datefmt=\'%Y/%m/%d %H:%M:%S\')" >> ' + analisis_script)
    lista_comandos.append('echo "    referencia = int(time())" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    proceso = Popen([\'cat '+ lista_ping +' | wc -l\'], shell=True, stdout=PIPE)" >> ' + analisis_script)
    lista_comandos.append('echo "    ips = int(proceso.stdout.read())" >> ' + analisis_script)
    lista_comandos.append('echo "    proceso.stdout.close()" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    cant_total_paquetes = ips * '+str(parametros.get('ping_sent'))+'" >> ' + analisis_script)
    lista_comandos.append('echo "    duracion = 3600" >> ' + analisis_script)
    lista_comandos.append('echo "    pps = min(1000,max(20, cant_total_paquetes/duracion))" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    comando = \'sudo /usr/local/bin/scamper -O planetlab -O warts -o ' + temp_ping + ' -p \' + str(pps) + \' -c \\\"ping -i 1 -m '+str(parametros.get('ping_ttl'))+' -c '+str(parametros.get('ping_sent'))+'\\\" ' + lista_ping + '\'" >> ' + analisis_script)
    lista_comandos.append('echo "    call([comando], shell=True)" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    comando = \'sudo /usr/local/bin/sc_wartscat -o ' + resultados + ' ' + temp_ping + '\' " >> ' + analisis_script)
    lista_comandos.append('echo "    call([comando], shell=True)" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    remove(\''+temp_ping+'\')" >> ' + analisis_script)
    lista_comandos.append('echo "    remove(\''+lista_ping+'\')" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    mensaje = \'PING - EJECUCION - PAQUETES: \'+str(cant_total_paquetes)+\' - PPS: \'+str(pps)+\' - DURACION: \' + str(int(time()) - referencia)" >> ' + analisis_script)
    lista_comandos.append('echo "    logging.info(mensaje)" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    return None" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    ### Main
    lista_comandos.append('echo "def main(): " >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    logging.basicConfig(filename=\'analisis'+ID_analisis+'.log\',level=logging.INFO, format=\'%(asctime)s, %(levelname)s, %(funcName)s, %(message)s\',  datefmt=\'%Y/%m/%d %H:%M:%S\')" >> ' + analisis_script)
    lista_comandos.append('echo "    logging.info(\'INICIO SCRIPT\')" >> ' + analisis_script)
    lista_comandos.append('echo "    try:" >> ' + analisis_script)
    lista_comandos.append('echo "        if path.exists(\'' + dir_destino + '\'):" >> ' + analisis_script)
    lista_comandos.append('echo "            tiempo_inicio_analisis = int(time())" >> ' + analisis_script)
    lista_comandos.append('echo "            ronda = 0" >> ' + analisis_script)
    lista_comandos.append('echo "            while True:" >> ' + analisis_script)
    lista_comandos.append('echo "                ronda += 1" >> ' + analisis_script)
    lista_comandos.append('echo "                tiempo_inicio_trace = int(time())" >> ' + analisis_script)
    lista_comandos.append('echo "                if ejecutar_traceroutes(str(ronda)):" >> ' + analisis_script)
    lista_comandos.append('echo "                    if (int(time()) - tiempo_inicio_analisis) > ' + duracion + ':" >> ' + analisis_script)
    lista_comandos.append('echo "                        break" >> ' + analisis_script)
    lista_comandos.append('echo "                    ejecutar_recalculo_TTL(str(ronda))" >> ' + analisis_script)
    lista_comandos.append('echo "                try:" >> ' + analisis_script)
    lista_comandos.append('echo "                    tamanio = path.getsize(\' \')/(1024 *1024)" >> ' + analisis_script)
    lista_comandos.append('echo "                    if tamanio > '+str(tamanio_maximo)+':" >> ' + analisis_script)
    lista_comandos.append('echo "                        logging.warning(\'Tamanio maximo superado [\'+tamanio+\' Mb]\')" >> ' + analisis_script)
    lista_comandos.append('echo "                        break" >> ' + analisis_script)
    lista_comandos.append('echo "                except OSError:" >> ' + analisis_script)
    lista_comandos.append('echo "                    pass" >> ' + analisis_script)
    lista_comandos.append('echo "                if (int(time()) - tiempo_inicio_analisis) > ' + duracion + ':" >> ' + analisis_script)
    lista_comandos.append('echo "                    break" >> ' + analisis_script)
    lista_comandos.append('echo "                aux = ' + periodo + ' - (int(time()) - tiempo_inicio_trace) " >> ' + analisis_script) # tiempo restante para ejecutar proximo trace
    lista_comandos.append('echo "                try:" >> ' + analisis_script)
    lista_comandos.append('echo "                    sleep(aux)" >> ' + analisis_script)
    lista_comandos.append('echo "                except:" >> ' + analisis_script)
    lista_comandos.append('echo "                    pass" >> ' + analisis_script)
    lista_comandos.append('echo "                " >> ' + analisis_script)
    lista_comandos.append('echo "            if \'' + parametros.get('ping_ejecutar') + '\' == \'S\':" >> ' + analisis_script)
    lista_comandos.append('echo "                generar_lista_ping()" >> ' + analisis_script)
    lista_comandos.append('echo "                ejecutar_ronda_ping()" >> ' + analisis_script)
    lista_comandos.append('echo "            else:" >> ' + analisis_script)
    lista_comandos.append('echo "                logging.info(\'PING - NO PROGRAMADO\')" >> ' + analisis_script)
    lista_comandos.append('echo "            comando = \'gzip -9 ' + resultados + '\'" >> ' + analisis_script)
    lista_comandos.append('echo "            call([comando], shell=True)" >> ' + analisis_script)
    lista_comandos.append('echo "            " >> ' + analisis_script)
    lista_comandos.append('echo "            remove(\'' + '0' + temp+ '\')" >> ' + analisis_script)
    lista_comandos.append('echo "            remove(\\\"'+dir_destino+'\\\")" >> ' + analisis_script)
    lista_comandos.append('echo "            " >> ' + analisis_script)
    lista_comandos.append('echo "        else:" >> ' + analisis_script)
    lista_comandos.append('echo "            logging.error(\'ARCHIVO DE DIRECCIONES NO HALLADO\')" >> ' + analisis_script)
    lista_comandos.append('echo "    except Exception, e:" >> ' + analisis_script)
    lista_comandos.append('echo "        logging.exception(e)" >> ' + analisis_script)
    lista_comandos.append('echo "    else:" >> ' + analisis_script)
    lista_comandos.append('echo "        logging.info(\'FIN SCRIPT\')" >> ' + analisis_script)
    lista_comandos.append('echo "    return None" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "main()" >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)
    lista_comandos.append('echo "    " >> ' + analisis_script)

    return '; '.join(lista_comandos)


