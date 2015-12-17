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

from psycopg2 import connect, Error as DatabaseError
from random import choice
from threading import Thread
from time import sleep
from getpass import getpass
from os import system
from os.path import dirname, abspath
from subprocess import PIPE, Popen
from func_BD import conectar_BD

directorio = dirname(abspath(__file__))
archivo_conf = directorio + '/files/program.conf'

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

def agregarNodos(api_server, auth, slice_name, nodos_agregar):
    """ ADD nodes in <nodos_agregar> to slice """

    try:
        api_server.AddSliceToNodes(auth, slice_name, nodos_agregar)
    except ErrorPlanetlab:
        raise

    return None


def cantidadNodos(cant_nodos, accion):
    """ Validate number of nodes to add or remove then """

    while True:

        if accion == 'agregar':
            cantidad_nodos_modificar = raw_input('Number of nodes to ADD: ')
        elif accion == 'eliminar':
            cantidad_nodos_modificar = raw_input('Number of nodes to REMOVE: ')

        if cantidad_nodos_modificar.isdigit():
            cantidad_nodos_modificar = int(cantidad_nodos_modificar)
            if cantidad_nodos_modificar >= 0 and cantidad_nodos_modificar <= cant_nodos:
                break
            else:
                print 'WRONG VALUE'
        else:
            print 'WRONG VALUE'

    return cantidad_nodos_modificar


def confirmar(texto):
    """ requests confirmation """

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


def instalarSoftware(slice_name, nodos, nodos_en_instalacion, nodos_ssh, nodos_funcionales, nodos_f14, sudoPassword):
    """ Installing on Planetlab Nodes the necessary packages for conducting explorations """

    print '\nStarting installation on selected nodes'
    threads = list()
    t = Thread(target=instalar_software_main_thread, args=(slice_name, nodos, nodos_en_instalacion, nodos_ssh, nodos_funcionales, nodos_f14, sudoPassword))
    threads.append(t)
    t.start()

    return None


def instalar_software_main_thread(slice_name, nodos, nodos_en_instalacion, nodos_ssh, nodos_funcionales, nodos_f14, sudoPassword):
    """ main thread """

    num_hilos = 4
    threads = list()
    hilos = []

    for x in nodos:
        while True:
            if x not in nodos_en_instalacion and len(hilos) <= num_hilos:
                hilos.append(1)
                nodos_en_instalacion.append(x)
                t = Thread(target=instalar_software_thread, args=(x, slice_name, nodos_en_instalacion, nodos_ssh, nodos_funcionales, nodos_f14, sudoPassword, hilos))
                threads.append(t)
                t.start()
                break
            else:
                sleep(1)

    return None

def instalar_software_thread(x, slice_name, nodos_en_instalacion, nodos_ssh, nodos_funcionales, nodos_f14, sudoPassword, hilos):
    """ thread """

    dir_destino = '/home/' + slice_name

    archivos_scamper = directorio + '/files/filesToInstall.tar.gz'
    archivo_script   = directorio + '/installNodes.sh'

    proceso = Popen(['parallel-scp','-H', x, '-l', slice_name, archivos_scamper, dir_destino], stdout=PIPE, stderr=PIPE)
    salida = proceso.stdout.read()
    proceso.stdout.close()

    proceso = Popen(['parallel-scp','-H', x, '-l', slice_name, archivo_script, dir_destino], stdout=PIPE, stderr=PIPE)
    salida = proceso.stdout.read()
    proceso.stdout.close()

    if x in nodos_f14:
        comando = 'python ' + directorio + '/sudo_access.py ' + slice_name + ' ' + config['publicKeyRSA'] + ' ' + config['ssh_pass'] + ' ' + x
        p = system('echo %s | sudo -S %s   >/dev/null 2>&1' % (sudoPassword, comando))

    proceso = Popen(['parallel-ssh','-t', '600','-H', x, '-l', slice_name, 'chmod +x ~/installNodes.sh; sh ~/installNodes.sh; exit'], stdout=PIPE, stderr=PIPE)
    salida = proceso.stdout.read()
    proceso.stdout.close()

    nodos_en_instalacion.remove(x)
    hilos.pop()

    ssh, scamper, fedora_release, midar = comprobar_funcionalidad(slice_name, x)
    if ssh == 'Si':
        nodos_ssh.append(x)
        if scamper == 'Si':
            nodos_funcionales.append(x)

    return None


def datosUsuario(api_server, auth):
    """ FIND USER SLICE """

    try:
        slice_data = api_server.GetSlices(auth, {}, ['name','node_ids'])

        print color.UNDERLINE + '\nSelect Slice to use:' + color.END
        print 'Nro  Name'.ljust(22) + '#Nodes'
        for n, x in enumerate(slice_data):
            print n+1,': ', x.get('name').ljust(13),'  ', len(x.get('node_ids'))

        while True:
            try:
                slice_usar = int(raw_input('\nSlice: '))
                if slice_usar in range(1,len(slice_data)+1):
                    slice_name = slice_data[slice_usar-1].get('name')
                    nodes_ids = slice_data[slice_usar-1].get('node_ids')
                    aux = api_server.GetNodes(auth, {'node_id': nodes_ids}, ['hostname'])
                    break
                else:
                    print 'WRONG'
            except TypeError:
                print 'WRONG'

    except KeyboardInterrupt:
        raise

    except ErrorPlanetlab:
        raise

    else:
        node_list = [x.get('hostname') for x in aux]

    return slice_name, node_list


def menuUsuarios(api_server, auth):
    """ User menu """

    while True:
        print color.UNDERLINE + '\nUser menu:' + color.END
        print '1: Users'
        print '2: Manual'
        print '3: Options'
        print '*: Exit'
        opcion = raw_input('\nOption: ')
        print '\n'

        if opcion == '1':     # User
            try:
                volver = True
                try:
                    usuarios = []
                    cargarUsuarios(usuarios)
                except:
                    print "Loading problems users"
                else:
                    if usuarios:
                        print color.UNDERLINE + 'User:' + color.END
                        cant_usuarios = len(usuarios)
                        n = 0
                        for x in usuarios:
                            n += 1
                            print n, ': ', x.get('usuario')

                        while True:
                            user_number = raw_input('\nSelect user:')

                            if user_number.isdigit():
                                user_number = int(user_number)

                                if user_number >= 1 and user_number <= cant_usuarios:
                                    n = 1
                                    for x in usuarios:
                                        if n == user_number:
                                            auth['Username'] = x.get('usuario')
                                            auth['AuthString'] = x.get('password')
                                            auth['AuthMethod'] = "password"
                                            volver = False
                                            continuar = True
                                            break
                                        n += 1
                                    break
                                else:
                                    print "Wrong user"
                            else:
                                print "Error logging user"

                if volver == False:
                    break

            except KeyboardInterrupt:
                pass

        elif opcion == '2':   # Manual entry

            try:
                auth['Username'] = raw_input('Entry user:\t')
                auth['AuthString'] = getpass('Entry pass:\t')
                auth['AuthMethod'] = "password"
                continuar = True
                break

            except KeyboardInterrupt:
                pass

        elif opcion == '3':   # User options

            while True:
                print color.UNDERLINE + 'User menu: User options:' + color.END
                print '1: View users'
                print '2: Add new user'
                print '3: Remove user'
                print '*: Exit'
                opt2 = raw_input('\nOption: ')

                if opt2 == '1':       # View users
                    try:
                        usuarios = []
                        cargarUsuarios(usuarios)
                    except Exception as e:
                        print 'ERROR: %s' % str(e)
                        
                    else:
                        if usuarios:
                            print '\nCurrent users:'
                            n = 0
                            for x in usuarios:
                                n += 1
                                print n, ': ', x.get('usuario')
                            print '\n'

                elif opt2 == '2':     # Add new user

                    print '\nEntry data (* to return):\n'
                    Username = raw_input('User:\t')
                    if Username != '*':
                        AuthString = getpass('Pass:\t')
                        auth['Username'] = Username
                        auth['AuthString'] = AuthString
                        auth['AuthMethod'] = "password"

                        if verificarUsuario(api_server, auth):
                            try:
                                conexion = connect(conectar_BD())
                                cursor = conexion.cursor()
                                cursor.execute('INSERT INTO users VALUES ' + str((Username, AuthString)) + ';')
                                conexion.commit()
                                cursor.close()
                                conexion.close()
                            except DatabaseError as e:
                                print '\nError: %s' % str(e)
                            else:
                                print '\nNew user added successfully\n'
                        else:
                            print '\nThe user was not validated by PlanetLab\n'

                elif opt2 == '3':     # Remove user

                    try:
                        usuarios = []
                        cargarUsuarios(usuarios)
                    except Exception as e:
                        print 'ERROR: %s' % str(e)
                    else:
                        if usuarios:
                            print '\nCurrent users:'
                            cant_usuarios = len(usuarios)
                            n = 0
                            for x in usuarios:
                                n += 1
                                print n, ': ', x.get('usuario')

                            while True:
                                user_number = raw_input('\nUser to remove (* to return): ')
                                if user_number == '*':
                                    break
                                elif user_number.isdigit():
                                    user_number = int(user_number)
                                    if user_number >= 1 and user_number <= cant_usuarios:
                                        try:
                                            conexion = connect(conectar_BD())
                                            cursor = conexion.cursor()
                                            aux = usuarios[user_number-1]
                                            cursor.execute('DELETE FROM users WHERE usr=\''+ str(aux.get('usuario')) +'\';')
                                            conexion.commit()
                                            cursor.close()
                                            conexion.close()
                                        except DatabaseError as e:
                                            print '\nError: %s' % str(e)
                                            break
                                        else:
                                            print '\nUser removed successfully\n'
                                            break
                                    else:
                                        print 'Wrong'
                                else:
                                    print 'Wrong'

                elif opt2 == '*':     # Return
                    break

        elif opcion == '*':   # Ending program
            auth['Username'] = ''
            auth['AuthString'] = ''
            auth['AuthMethod'] = "password"
            continuar = False
            break

    return continuar


def cargarUsuarios(usuarios):
    """ Take user from USERS table """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()
        cursor.execute("""SELECT * FROM users;""")
        registros = cursor.fetchall()
        [usuarios.append({'usuario': x[0], 'password': x[1]}) for x in registros]
        cursor.close()
        conexion.close()
    except DatabaseError:
        print 'No users registered\n'

    return None

def nodosNoFuncionales(slice_name, nodos):
    """ Generate a list of nodes without SSH conexion """

    print 'Generating list of nodes to remove'
    node_list_to_remove = []
    hilos = []
    num_hilos = 5
    threads = list()
    for x in nodos:
        while True:
            if len(hilos) <= num_hilos:
                hilos.append(1)
                t = Thread(target=nodos_no_funcionales_thread, args=(x,slice_name,node_list_to_remove,hilos))
                threads.append(t)
                t.start()
                break
            else:
                sleep(1)

    while hilos:
        sleep(1)

    return node_list_to_remove


def nodos_no_funcionales_thread(x, slice_name, node_list_to_remove, hilos):
    """ thread """

    ssh, scamper, fedora_release, midar = comprobar_funcionalidad(slice_name, x)
    if ssh == 'No':
        print 'Node:\t', x
        node_list_to_remove.append(x)

    hilos.pop()

    return None


def removerNodos(api_server, auth, slice_name, nodos_remover):
    """ Remove <nodes_to_remove> nodes from Slice """

    try:
        api_server.DeleteSliceFromNodes(auth, slice_name, nodos_remover)
    except ErrorPlanetlab:
        raise

    return None


def seleccion_nodos_planetlab_especificos(api_server, auth, node_list, max_nodes):
    """ List of specific PlanetLab nodes indicated by user """

    cant_max_nodos = max_nodes - len(node_list)

    print 'Entry ID of nodes to add ( * to finish ):'
    aux = ''
    n = 1
    lista_ID = []
    while aux != '*' and n <= cant_max_nodos:
        aux  = raw_input('Node:\t')
        n += 1
        if aux.isdigit():
            lista_ID.append(int(aux))

    try:
        lista = api_server.GetNodes(auth, {'node_id': lista_ID}, ['hostname'])
    except ErrorPlanetlab:
        raise

    lista_hostname = [x.get('hostname') for x in lista]

    if lista_hostname:
        print '\nNodes selected:'
        verNodos(api_server, auth, lista_hostname)
    else:
        print '\nNot found nodes selected'

    return lista_hostname


def seleccion_nodos_slice_especificos(api_server, auth, node_list, mensaje = ''):
    """ List of specific nodes indicated by user """

    print mensaje
    verNodos(api_server, auth, node_list)

    print 'Entry ID ( * to finish ):'
    aux = ''
    lista_ID = []
    while aux != '*':
        aux  = raw_input('Node:\t')
        if aux.isdigit():
            lista_ID.append(int(aux))
    try:
        lista = api_server.GetNodes (auth, {'node_id': lista_ID}, ['hostname'])
    except ErrorPlanetlab:
        raise

    lista_hostname = [x.get('hostname') for x in lista if x.get('hostname') in node_list]

    return lista_hostname


def seleccion_nodos_planetlab_aleatorios(api_server, auth, node_list, cantidad_nodos_agregar):
    """ List of random PlanetLab nodes """

    nodos_seleccionados = []
    new_sites = []

    try:
        node_filter = {}
        node_filter['hostname'] = node_list
        aux = api_server.GetNodes(auth, node_filter, ['site_id'])
        current_sites = [x.get('site_id') for x in aux]

        node_filter = {}
        node_filter['boot_state'] = 'boot'
        node_filter['~last_boot'] = 0
        aux = api_server.GetNodes(auth, node_filter, ['hostname', 'site_id'])
        nodes_planetlab = [[x.get('hostname'), x.get('site_id')] for x in aux]

        threads = list()
        control = []
        indice = [n for n in range(len(nodes_planetlab))]

        for x in range(cantidad_nodos_agregar):
            control.append(x)
            t = Thread(target=seleccion_nodos_planetlab_aleatorios_thread, args=(x, node_list, nodes_planetlab,nodos_seleccionados, current_sites, new_sites, indice, control))
            threads.append(t)
            t.start()

        while control:
            sleep(1)

    except ErrorPlanetlab:
        raise

    return nodos_seleccionados

def seleccion_nodos_F14(api_server, auth, node_list, cant_nodos_agregar):
    """ List of random Fedora 14 nodes from <nodos_fed14.txt> """

    nodos_seleccionados = []

    try:
        archivo = open( directorio + '/files/nodos_fed14.dat','r')
        nodos = archivo.read()
        nodos = nodos.split('\n')
        hostname_nodos_F14 = set()
        [hostname_nodos_F14.add(x) for x in nodos if x]
        hostname_nodos_F14 = list(hostname_nodos_F14 - set(node_list))
        archivo.close()
    except:
        print 'WARNING: Problem to load "nodos_fed14.dat"'
    else:
        node_filter = {}
        node_filter['hostname'] = hostname_nodos_F14
        node_filter['boot_state'] = 'boot'
        aux = api_server.GetNodes(auth, node_filter, ['hostname'])

        try:
            for x in aux[:cant_nodos_agregar]:
                nodos_seleccionados.append(x.get('hostname'))
        except IndexError:
            pass

    return nodos_seleccionados

def seleccion_nodos_planetlab_aleatorios_thread(x, node_list, nodes_planetlab, nodos_seleccionados, current_sites, new_sites, indice, control):
    """ List of random PlanetLab nodes and verify if they response to a ping """

    while True:
        nodo = choice(indice)
        indice.remove(nodo)
        nodo_random = nodes_planetlab[nodo]
        if nodo_random[1] not in current_sites and nodo_random[1] not in new_sites:
            response = system("ping -c 1 -W 10 " + nodo_random[0] + "  > /dev/null  2> /dev/null")
            if response == 0: # if node does not response to ping
                nodos_seleccionados.append(nodo_random[0])
                new_sites.append(nodo_random[1])
                break

    control.remove(x)

    return None


def seleccion_nodos_slice_aleatorios(node_list, cantidad_nodos):
    """ List of random <cantidad_nodos> nodes from slice """

    nodos = []
    for x in range(cantidad_nodos):
        while True:
            nodo_random = choice(node_list)
            if nodo_random not in nodos:
                nodos.append(nodo_random)
                break

    return nodos


def lista_nodos_no_eliminables():
    """ List of nodes which are not currently possible to remove """

    try:
        conexion = connect(conectar_BD())
        cursor = conexion.cursor()

        cursor.execute("""SELECT nodes FROM analysis where stored='N';""")
        reg = cursor.fetchall()

        nodos = []
        [nodos.extend(x[0].split()) for x in reg]
        nodos = list(set(nodos))

        cursor.close()
        conexion.close()

    except IOError:
        pass

    except DatabaseError:
        pass

    return nodos


def verNodos(api_server, auth, node_list):
    """ Display information related to nodes in <node_list> """

    try:
        datos = api_server.GetNodes (auth, {'hostname': node_list}, ['node_id','hostname','site_id','boot_state'])
    except ErrorPlanetlab:
        raise

    print color.BOLD + '\nID\tHostname\t\t\tSite_ID' + color.END

    for x in datos:
        hostname = x.get('hostname')[:30]
        while len(hostname) < 30:
            hostname += ' '

        print x.get('node_id'),'\t',hostname,'\t',x.get('site_id')

    return None


def verificarUsuario(api_server, auth):
    """ Verifies the user against PlanetLab  """

    print '\nVerifying user...'
    try:
        if api_server.AuthCheck(auth) == 1:
            verificacion = True
    except:
        verificacion = False

    return verificacion


def comprobar(api_server, auth, node_list, slice_name):
    """ Checking information of nodes in <node_list> and return a list of useful nodes """

    try:
        datos = api_server.GetNodes (auth, {'hostname': node_list}, ['node_id','hostname','site_id','boot_state'])
    except ErrorPlanetlab:
        raise

    print color.BOLD + '\nID\tHostname\t\t\tSite_ID\tState\tFedora\tSSH\tScamper\tMIDAR' + color.END

    threads = list()
    nodos_funcionales = []
    nodos_ssh = []
    nodos_f14 = []
    nodos_midar = []
    control = [x for x in datos]
    hilos = []

    try:
        for x in datos:
            while True:
                if len(hilos) <= int(config['num_thread']):
                    hilos.append(1)
                    t = Thread(target=comprobar_thread, args=(x,slice_name,nodos_funcionales,nodos_ssh,nodos_f14,nodos_midar,control,hilos))
                    threads.append(t)
                    t.start()
                    break
                else:
                    sleep(1)
    
        while hilos:
            sleep(1)

    except KeyboardInterrupt:
        pass

    print '\n'

    return nodos_funcionales, nodos_ssh, nodos_f14, nodos_midar


def comprobar_thread(x,slice_name,nodos_funcionales,nodos_ssh,nodos_f14,nodos_midar,control,hilos):
    """ thread """

    hostname = x.get('hostname')[:30]

    while len(hostname) < 30:
        hostname += ' '

    ssh, scamper, fedora_release, midar = comprobar_funcionalidad(slice_name, x.get('hostname'))

    if ssh == 'Si':
        nodos_ssh.append(x.get('hostname'))
        if scamper == 'Si':
            nodos_funcionales.append(x.get('hostname'))
        if fedora_release == '14':
            nodos_f14.append(x.get('hostname'))
        if midar == 'Si':
            nodos_midar.append(x.get('hostname'))


    registro = '\n' + str(x.get('node_id')) + '\t' + hostname + '\t' + str(x.get('site_id'))[:4] + '\t' + x.get('boot_state') + '\t' + fedora_release + '\t' + ssh + '\t' + scamper + '\t' + midar + '\t' + str(len(control))

    print registro

    control.remove(x)
    hilos.pop()

    return None


def comprobar_funcionalidad(slice_name, node):
    """ Check if a node has SSH access and Scamper installed """

    proceso = Popen(['parallel-ssh','-P','-t', config['ssh_time_out'], '-H', node, '-l', slice_name, 'whereis scamper; cat /etc/fedora-release; whereis mper; if [ -d "/home/'+slice_name+'/'+config['midar_folder']+'" ]; then echo "Y"; else echo "N"; fi'], stdout=PIPE)
    salida = proceso.stdout.readlines()
    proceso.stdout.close()

    try:
        if '[SUCCESS]' in salida[-1]: # Is there connection?
            ssh = 'Si'
    
            if '/usr/local/bin/scamper' in salida[0]: # Is Scamper installed?
                scamper = 'Si'
            else:
                scamper = '-'
    
            if 'Fedora release 14' in salida[1]: # Which Fedora distribution is?
                fedora_release = '14'
                if 'Y' in salida[3]: # Is MIDAR installed?
                    if '/usr/local/bin/mper' in salida[2]:
                        midar = 'Si'
                    else:
                        midar = 'No'
                else:
                    midar = 'No'
            elif 'Fedora release 8' in salida[1]:
                fedora_release = '8'
                midar = '-'
            else:
                fedora_release = 'D'
                midar = '-'
    
        else:
            ssh = 'No'
            scamper = '-'
            fedora_release = '-'
            midar = '-'
            
    except IndexError: 
        ssh = 'No'
        scamper = '-'
        fedora_release = '-'
        midar = '-'
            
    
    return ssh, scamper, fedora_release, midar
