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

from func_topology import programarResolucionAliases, obtenerEstado
from func_BD import conectar_BD, generar_ID_analisis, registrarAnalisis
from func_admin import verNodos, confirmar
from func_analysis import mostrarRegistro, cancelarAnalisis, almacenarMediciones, eliminarResultados, \
elegirOrigen, elegirDestino, obtenerParametros, configuracionAnalisis, resumenConf, instalarAnalisis, \
instalarAnalisis_B2B, obtenerParametros_B2B

mensajeErrorConexion = '\n--- Conexion Problem---\n'
mensajeEnter = '\n Press Intro to continue \n'

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


def analisis(api_server, auth, slice_name, node_list, nodos_funcionales, nodos_ssh, nodos_f14, nodos_midar):
    """ Administration of explorations """

    while True:
        print color.UNDERLINE + 'Main menu: Explorations:' + color.END
        print '1: View functional nodes'
        print '2: Summery of last explorations'
        print '3: New exploration'
        print '4: New Traceroutes B2B'
        print '5: Cancel exploration'
        print '6: Store'
        print '7: Alias resolution'
        print '8: Delete data'
        print '*: Exit'
        opcion = raw_input('\nOpcion: ')
        print '\n'

        if opcion == '1':           # View functional nodes
            try:
                if nodos_funcionales:
                    print 'Usable nodes:'
                    verNodos(api_server, auth, nodos_funcionales)
                else:
                    print 'No usable nodes'
                print '\n'
            except ErrorPlanetlab:
                print mensajeErrorConexion
                raw_input(mensajeEnter)

        elif opcion == '2':         # Summery of last explorations
            if not (mostrarRegistro('almacenamiento', 'y') or mostrarRegistro('almacenamiento', 'y', 'B2B')):
                print 'Empty\n'
            if not (mostrarRegistro('almacenamiento', 'n') or mostrarRegistro('almacenamiento', 'n', 'B2B')):
                print 'Empty\n'

        elif opcion == '3':         # New exploration
            try:
                parametros = {}
                parametros['usuario'] = auth.get('Username')
                parametros['slice_name'] = slice_name
                parametros['motivo'] = 'scamper'

                # Select origin nodes from where exploration will run
                parametros['nodos_origen'] = elegirOrigen(api_server, auth, nodos_funcionales)
                if parametros.get('nodos_origen'):
                    # Select IP target
                    parametros['nodos_destino'] = elegirDestino(api_server, auth, parametros)
                    if parametros.get('nodos_destino'):
                        # Config exploration
                        generar_ID_analisis(parametros)
                        obtenerParametros(parametros)
                        parametros['descripcion'] = raw_input('Description: \t')
                        configuracion_analisis = configuracionAnalisis(parametros)
                        resumenConf(parametros)
                        if confirmar('Excecute exploration'):
                            # Transfer files and start it
                            instalarAnalisis(parametros, configuracion_analisis)
                            # Exploration registered
                            registrarAnalisis(parametros)

            except KeyboardInterrupt:
                pass

            except ErrorPlanetlab:
                print mensajeErrorConexion
                raw_input(mensajeEnter)

        elif opcion == '4':         # B2B new exploration

            try:
                parametros = {}
                parametros['usuario'] = auth.get('Username')
                parametros['slice_name'] = slice_name
                parametros['motivo'] = 'B2B'

                parametros['nodos_origen'] = elegirOrigen(api_server, auth, nodos_funcionales)
                if parametros.get('nodos_origen'):
                    parametros['nodos_destino'] = elegirDestino(api_server, auth, parametros)
                    if parametros.get('nodos_destino'):                    
                        generar_ID_analisis(parametros)
                        obtenerParametros_B2B(parametros)
                        parametros['descripcion'] = raw_input('Description: \t')
                        resumenConf(parametros)
                        if confirmar('Excecute exploration'):
                            # Transfer files and start it
                            instalarAnalisis_B2B(parametros)
                            # Exploration registered
                            registrarAnalisis(parametros)

            except KeyboardInterrupt:
                pass

            except ErrorPlanetlab:
                print mensajeErrorConexion
                raw_input(mensajeEnter)

            break


        elif opcion == '5':         # Cancel exploration
            if mostrarRegistro('almacenamiento', 'n'):
                while True:
                    try:
                        ID = raw_input('\nEnter ID:\t')
                        if ID.isdigit():
                            cancelarAnalisis(auth.get('Username'), slice_name, node_list, int(ID))
                            break
                        else:
                            print '\tID Wrong'
                    except KeyboardInterrupt:
                        break
            else:
                print 'No running exploration'

        elif opcion == '6':         # Stored exploration
            if mostrarRegistro('almacenamiento', 'n') or mostrarRegistro('almacenamiento', 'n', 'B2B'):
                while True:
                    try:
                        ID = raw_input('\nEnter ID:\t')
                        if ID.isdigit():
                            almacenarMediciones(auth, slice_name, node_list, int(ID))
                            break
                        else:
                            print '\tID Wrong'
                    except KeyboardInterrupt:
                        break
            else:
                print 'No running exploration'

        elif opcion == '7':         # Alias resolution

            if nodos_midar:
                while True:
                    try:
                        # Select exploration to resolve
                        ID = raw_input('\nEnter ID (* to return):\t')
                        if ID == '*':
                            break

                        elif ID.isdigit():
                            try:
                                estado = obtenerEstado(ID)
                            except:
                                estado = '-1'

                            # List of possible states:
                                # 0: Topology unresolved
                                # 1: Solving topology on a single node -> Previo a estado final
                                # 2: Running estimation state across multiple nodes -> Estado intermedio
                                # 3: Solving topology across multiple nodes -> Previo a estado final
                                # 4: Topology resolved

                            if estado in ('0', '1', '2', '3'):
                                programarResolucionAliases(ID, api_server, auth, slice_name, nodos_midar, estado)
                                break

                            elif estado == '4':
                                print 'Alias previously resolved\n'

                            elif estado == '9':
                                print 'Alias resolution is not implemented for this type of exploration\n'
                            else:
                                print 'ID not registered'

                    except KeyboardInterrupt:
                        break

            else:
                print 'No MIDAR nodes available'

        elif opcion == '8':         # Delete data
             while True:
                 print color.UNDERLINE + 'Main menu: Explorations: Delete data:' + color.END
                 print '1: Delete exploration'
                 print '2: Delete alias resolution data'
                 print '*: Exit'
                 opcion = raw_input('\nOption: ')
                 print '\n'

                 if opcion == '1':
                    if mostrarRegistro('almacenamiento', 'y'):
                        while True:
                            try:
                                ID = raw_input('\nEnter ID:\t')
                                if ID.isdigit():
                                    eliminarResultados(ID, 'analysis')
                                    break
                                else:
                                    print '\tID Wrong'
                            except KeyboardInterrupt:
                                break
                    else:
                        print 'No datos para eliminar'

                 elif opcion == '2':
                    if mostrarRegistro('almacenamiento', 'y'):
                        while True:
                            try:
                                ID = raw_input('\nEnter ID:\t')
                                if ID.isdigit():
                                    eliminarResultados(ID, 'alias_resolution')
                                    break
                                else:
                                    print '\tID Wrong'

                            except KeyboardInterrupt:
                                break
                    else:
                        print 'No data to delete'

                 elif opcion == '*':       # Volver
                     break

        elif opcion == '*':
            break

    return None

