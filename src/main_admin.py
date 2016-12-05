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

from func_admin import verNodos, agregarNodos, cantidadNodos, seleccion_nodos_planetlab_especificos, \
seleccion_nodos_planetlab_aleatorios, seleccion_nodos_F14, instalarSoftware, \
seleccion_nodos_slice_especificos, lista_nodos_no_eliminables, seleccion_nodos_slice_aleatorios, \
nodosNoFuncionales, removerNodos, datosUsuario

from func_BD import cargarDatosDirecciones, actualizarDatosNodos

from func_analysis import ingresarPassSudo

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

def admin(api_server, auth, slice_name, node_list, nodos_funcionales, nodos_ssh, nodos_f14, nodos_midar, nodos_en_instalacion):
    """ Administration of nodes """

    while True:
        print color.UNDERLINE + 'Main menu: Administration:' + color.END
        print '1: View nodes'
        print '2: Add nodes'
        print '3: Install software in nodes'
        print '4: Remove nodes'
        print '5: Update information of Planetlab nodes'
        print '6: Update table of ipv4 address blocks'
        print '*: Exit'
        opcion = raw_input('\nOption: ')
        print '\n'

        if opcion == '1':      # View nodes
            try:
                verNodos(api_server, auth, node_list)
                raw_input(mensajeEnter)
            except ErrorPlanetlab:
                print mensajeErrorConexion
                raw_input(mensajeEnter)
                break

        elif opcion == '2':    # Add nodes
             while True:
                 print color.UNDERLINE + 'Main menu: Administration: Add nodes:' + color.END
                 print '1: Add by ID'
                 print '2: Add randomly'
                 print '3: Add only Fedora 14'
                 print '*: Exit'
                 opcion = raw_input('\nOption: ')
                 print '\n'

                 if opcion in ('1', '2', '3'):
                     try:
                         max_nodes = 100
                         nodes_free = 100

                         if opcion == '1': # Add by ID
                             lista_nodos_agregar = seleccion_nodos_planetlab_especificos(api_server, auth, node_list, max_nodes)

                         elif opcion == '2': # Add randomly
                             cant_nodos_agregar = cantidadNodos(nodes_free, 'agregar')
                             if cant_nodos_agregar:
                                 lista_nodos_agregar = seleccion_nodos_planetlab_aleatorios(api_server, auth, node_list, cant_nodos_agregar)

                         elif opcion == '3': # Add only Fedora 14
                             cant_nodos_agregar = cantidadNodos(nodes_free, 'agregar')
                             if cant_nodos_agregar:
                                 lista_nodos_agregar = seleccion_nodos_F14(api_server, auth, node_list, cant_nodos_agregar)
                         else:
                             pass

                         # Agrego los nodos seleccionados al slice
                         if lista_nodos_agregar:
                             print 'Selected Nodes:\n'
                             for x in lista_nodos_agregar:
                                 print x

                             agregarNodos(api_server, auth, slice_name, lista_nodos_agregar)
                             for x in lista_nodos_agregar:
                                 node_list.append(x)
                             print '\nNodes added\n'

                         else:
                             print 'No node was selected\n'

                     except ErrorPlanetlab:
                         print mensajeErrorConexion
                         raw_input(mensajeEnter)
                         break

                 elif opcion == '*':
                     break

        elif opcion == '3':    # Install software
             while True:
                 print color.UNDERLINE + 'Main menu: Administration: Install software:' + color.END
                 print '1: View instalation state'
                 print '2: Install in specific nodes'
                 print '3: Install in nodes without scamper'
                 print '4: Install in all nodes'

                 print '*: Exit'
                 opcion = raw_input('\nOption: ')

                 if opcion == '1':         # View instalation state
                     try:
                         if nodos_en_instalacion:
                             print '\nNodes running instalation script:'
                             verNodos(api_server, auth, nodos_en_instalacion)
                         else:
                             print '\nNodes running instalation script: Empty\n'

                     except ErrorPlanetlab:
                         print mensajeErrorConexion
                         raw_input(mensajeEnter)
                         break

                 elif opcion == '2':       # Install in specific nodes
                     try:
                         # List of nodes with SSH access and is not install Scamper
                         nodos_preinstalacion = [x for x in nodos_ssh if x not in nodos_funcionales and x not in nodos_en_instalacion]

                         # List of nodes where it will be install software
                         if nodos_preinstalacion:
                             mensaje = 'Nodes with SSH access and not Scamper installed:'
                             lista_instalacion = seleccion_nodos_slice_especificos(api_server, auth, nodos_preinstalacion, mensaje)

                             if set(lista_instalacion) & set(nodos_f14):
                                 sudoPassword = ingresarPassSudo()
                             else:
                                 sudoPassword = None

                             instalarSoftware(slice_name, lista_instalacion, nodos_en_instalacion, nodos_ssh, nodos_funcionales, nodos_f14, sudoPassword)

                         else:
                             print 'There is not nodes in where install software'

                     except ErrorPlanetlab:
                         print mensajeErrorConexion
                         raw_input(mensajeEnter)
                         break

                 elif opcion == '3':       # Instalar en todos los nodos sin scamper
                     try:
                         lista_instalacion = [x for x in nodos_ssh if x not in nodos_funcionales and x not in nodos_en_instalacion]

                         if lista_instalacion:
                             if set(lista_instalacion) & set(nodos_f14):
                                 sudoPassword = ingresarPassSudo()
                             else:
                                 sudoPassword = None
                             instalarSoftware(slice_name, lista_instalacion, nodos_en_instalacion, nodos_ssh, nodos_funcionales, nodos_f14, sudoPassword)
                         else:
                             print 'No hay nodos en los que instalar software'

                     except ErrorPlanetlab:
                         print mensajeErrorConexion
                         raw_input(mensajeEnter)
                         break

                 elif opcion == '4':       # Instalar en todos los nodos
                     try:
                         lista_instalacion = [x for x in nodos_ssh]

                         if lista_instalacion:
                             if set(lista_instalacion) & set(nodos_f14):
                                 sudoPassword = ingresarPassSudo()
                             else:
                                 sudoPassword = None
                             instalarSoftware(slice_name, lista_instalacion, nodos_en_instalacion, nodos_ssh, nodos_funcionales, nodos_f14, sudoPassword)
                         else:
                             print 'No hay nodos en los que instalar software'

                     except ErrorPlanetlab:
                         print mensajeErrorConexion
                         raw_input(mensajeEnter)
                         break
                 elif opcion == '*':       # Volver
                     break

        elif opcion == '4':    # Remove nodes

            # List of nodes where currently is running an exploration
            nodos_trabajando = lista_nodos_no_eliminables()

            while True:
                print color.UNDERLINE + 'Main menu: Administration: Remove nodes:' + color.END
                print '1: Remove nodes without SSH'
                print '2: Remove specfic node'
                print '3: Remove randomly'
                print '4: Remove all nodes'
                print '*: Exit'

                opcion = raw_input('\nOption: ')
                print '\n'

                if opcion == '1':    # Remove nodes without SSH
                    try:
                        # List of nodes without SSH access
                        node_list_to_remove = list(set(node_list) - set(nodos_ssh))
                        # Remove selected nodes
                        removerNodos(api_server, auth, slice_name, node_list_to_remove)
                        print 'Removing...'
						
                        if node_list_to_remove:
                            # Update list
                            node_list = list(set(node_list) - set(nodos_ssh))
                            print 'Nodes removed successfully'
                        else:
                            print 'No nodes without access'
                    except ErrorPlanetlab:
                        print mensajeErrorConexion
                        raw_input(mensajeEnter)
                        break

                elif opcion == '2':  # Remove specfic node
                    try:
                        print 'Currently nodes:\n'
                        verNodos(api_server, auth, node_list)

                        node_list_to_remove = []
                        aux = ''
                        print('ID of nodes to remove ( * to finish ):\n')
                        while aux != '*':
                            aux  = raw_input('Node:\t')
                            if aux.isdigit():
                                node_list_to_remove.append(int(aux))
                            else:
                                if aux != '*':
                                    print '\tWrong'

                        if node_list_to_remove:
                            while True:
                                eliminar = raw_input('Confirm remove nodes selected: (y/n)\t')
                                if eliminar.lower() == 'y':
                                    removerNodos(api_server, auth, slice_name, node_list_to_remove)
                                    #slice_name, node_list = datosUsuario(api_server, auth)
                                    break
                                elif eliminar.lower() == 'n':
                                    break

                    except ErrorPlanetlab:
                        print mensajeErrorConexion
                        raw_input(mensajeEnter)
                        break

                elif opcion == '3':
                    try:
                         # List of nodes which will be removed
                         cant_nodos_eliminar = cantidadNodos(len(node_list), 'eliminar')

                         # Remove randomly nodes from Slice
                         lista_nodos_eliminar = seleccion_nodos_slice_aleatorios(node_list, cant_nodos_eliminar)

                         print 'Nodes selected: ', lista_nodos_eliminar

                         if lista_nodos_eliminar:
                             while True:
                                 eliminar = raw_input('Confirm remove all nodes: (y/n)\t')
                                 if eliminar.lower() == 'y':

                                     nodos_no_eliminables = []
                                     for x in lista_nodos_eliminar:
                                         if x in nodos_trabajando:
                                             lista_nodos_eliminar.remove(x)
                                             nodos_no_eliminables.append(x)

                                     if nodos_no_eliminables:
                                         print 'Next nodes cannot be removed:'
                                         print nodos_no_eliminables
                                         print 'These nodes must be remove manually\n'

                                     if lista_nodos_eliminar:
                                         removerNodos(api_server, auth, slice_name, lista_nodos_eliminar)
                                         #slice_name, node_list = datosUsuario(api_server, auth)
                                         print 'Nodes removed successfully\n'
                                         break
                                     else:
                                         print 'No nodes to remove\n'
                                         break

                                 elif eliminar.lower() == 'n':
                                     break

                    except ErrorPlanetlab as e:
                        print 'ERROR: %s' % str(e)
                        raw_input(mensaje_enter)
                        break

                elif opcion == '4':  # Remove all nodes
                    try:
                        while True:
                            eliminar = raw_input('Confirm to remove all nodes: (y/n)\t')
                            if eliminar.lower() == 'y':
                                nodos_eliminar = [x for x in node_list if x not in nodos_trabajando]
                                if nodos_trabajando:
                                    print 'Next nodes cannot be removed:'
                                    print nodos_trabajando
                                    print '\nThese nodes must be remove manually\n'
                                removerNodos(api_server, auth, slice_name, nodos_eliminar)
                                #slice_name, node_list = datosUsuario(api_server, auth)
                                for x in nodos_eliminar:
                                    try:
                                        nodos_funcionales.remove(x)
                                        nodos_ssh.remove(x)
                                    except:
                                        pass
                                break
                            elif eliminar.lower() == 'n':
                                break
                    except ErrorPlanetlab:
                        print mensajeErrorConexion
                        raw_input(mensajeEnter)
                        break

                elif opcion == '*':
                    break

        elif opcion == '5':    # Update information of Planetlab nodes
            actualizarDatosNodos(api_server, auth, slice_name)

        elif opcion == '6':    # Update table of ipv4 address blocks (MaxMind)
            cargarDatosDirecciones('actualizar')

        elif opcion == '*':    # Return to user menu
            break

    return None



