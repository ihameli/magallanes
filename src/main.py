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

from xmlrpclib import ServerProxy
from os import system, getcwd, path, makedirs
from os.path import exists, dirname, abspath
from sys import exit
from imp import find_module
from subprocess import call, PIPE, Popen

from func_BD import checkDB
from func_admin import menuUsuarios, verificarUsuario, datosUsuario, comprobar, confirmar
from main_analysis import analisis
from main_admin import admin

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

def checkStart():
    """ Checking files, libraries and packages """

    status = True

    print '\n'

    # CHECKING FILES
    archivos = set(['program.conf', 'GeoLite2-City.mmdb', 'GeoLite2-Country-Locations-en.csv'])
    archivosNoEncontrados = set()

    for x in archivos:
        if not exists(dirname(abspath(__file__)) + '/files/' + x):
            archivosNoEncontrados.add(x)
            status = False

    if archivosNoEncontrados:
        print '- Files not found: '
        for x in archivosNoEncontrados:
            print '   ', x
        print '\n'

    # CHECKING FOLDERS
    if not path.exists(dirname(abspath(__file__)) + '/temp'):
        makedirs(dirname(abspath(__file__)) + '/temp')

    # CHECKING LIBRARIES
    bibliotecas = set(['xmlrpclib', 'psycopg2', 'netaddr', 'json', 'pexpect', 'geoip2'])
    bibliotecasNoInstaladas = set()

    for x in bibliotecas:
        try:
            find_module(x)
        except ImportError:
            bibliotecasNoInstaladas.add(x)
            status = False

    if bibliotecasNoInstaladas:
        print '- Libraries not found:'
        for x in bibliotecasNoInstaladas:
            print '   ', x
        print '\n'

    # CHECKING PROGRAMS
    programas = set(['scamper','postgresql','sc_warts2json', 'parallel-ssh'])
    programasNoInstalados = set()

    for x in programas:
        proceso = Popen(['whereis ' + x], shell=True, stdout=PIPE)
        salida = proceso.stdout.read().split(':')
        proceso.stdout.close()

        if x not in salida[1]:
            programasNoInstalados.add(x)
            status = False

    if programasNoInstalados:
        print '- Programs not found:'
        for x in programasNoInstalados:
            print '   ', x
        print '\n'

    return status


def main():
    """ main """

    system('clear')
    api_server = ServerProxy('https://www.planet-lab.org/PLCAPI/')
    print color.BOLD + '\n\n--- MAGALLANES: INTERNET TOPOLOGY EXPLORER ---\n' + color.END

    while True:

        # CHECKING DB CONEXION
        if checkDB():

            auth = {} # Dictionary for communite to

            # User menu -> Manage users, or exit
            if menuUsuarios(api_server, auth):

                # Validating user on PlanetLab
                if verificarUsuario(api_server, auth):
                    try:
                        try:
                            slice_name, node_list = datosUsuario(api_server, auth)
                        except KeyboardInterrupt:
                            raise

                        print '\nUSER ACCEPTED: %s ( %s )' % (auth['Username'], slice_name)

                        print '\nPreparing nodes...'
                        nodos_funcionales, nodos_ssh, nodos_f14, nodos_midar = comprobar(api_server, auth, node_list, slice_name)
                        nodos_en_instalacion = []

                    except KeyboardInterrupt:
                        pass

                    except ErrorPlanetlab:
                        print mensajeErrorConexion
                        break

                    else:
                        while True:
                            # Main menu
                            print color.UNDERLINE + 'Main menu:' + color.END
                            print '1: Admin Nodes'
                            print '2: Admin Explorations'
                            print '*: Exit'
                            opcion = raw_input('\nOption: ')
                            print '\n'

                            # Administration
                            if opcion == '1':
                                admin(api_server, auth, slice_name, node_list, nodos_funcionales, nodos_ssh, nodos_f14, nodos_midar, nodos_en_instalacion)

                            # Explorations
                            elif opcion == '2':
                                analisis(api_server, auth, slice_name, node_list, nodos_funcionales, nodos_ssh, nodos_f14, nodos_midar)

                            # Exit
                            elif opcion == '*':
                                if nodos_en_instalacion:
                                    print color.BOLD + 'Warning:' + color.END + ' Remain nodes in software installation procedures'
                                if confirmar('Exit'):
                                    break

                else:
                    print '\nUser was not verified by PlanetLab'

            else:
                break

        else:
            print '\nDATABASE CONEXION FAILED'
            raw_input(mensajeEnter)
            break

    print color.BOLD + '--- BYE ---\n' + color.END
    system('clear')

    return None

# START #
if __name__ == '__main__':
    try:
        if checkStart():
            status = main()
            exit(status)
    except KeyboardInterrupt:
        print color.BOLD + '\n\n ---- PROGRAM INTERRUPTED ---\n\n' + color.END
        exit()
