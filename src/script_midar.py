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

from sys import argv
from os import system, listdir, rename
from time import sleep
from subprocess import PIPE, Popen, call

import logging

# INPUT PARAMETERS
slice_name =     argv[1]
hostname =       argv[2]
user =           argv[3]
ID =             argv[4]
opcion =         argv[5]
mper_port =      argv[6]
mper_pps =       argv[7]
est_duration =   argv[8]
est_rounds =     argv[9]
elim_rounds =    argv[10]
cor_rounds =     argv[11]
est_overlap =    argv[12]
disc_overlap =   argv[13]
elim_overlap =   argv[14]
cor_overlap =    argv[15]

# WORK DIRECTORY
dir_resultados = '/home/%s/%s/' % (slice_name, user)

# MAIN
def main():
    """ CONFIGURATION AND MANAGING OF EXPLORATION """

    # PREPARING LOG FILE
    logging.basicConfig(filename='/home/%s/%s/MIDAR.log' % (slice_name, user),level=logging.INFO, format='%(asctime)s, %(levelname)s, %(funcName)s, %(message)s',  datefmt='%Y/%m/%d %H:%M:%S')

    logging.info('BEGIN')
    logging.info('ID: %s' % ID)

    # SEARCHING MIDAR FOLDER
    ejecutar = False
    for x in listdir('/home/%s' % slice_name):
        if 'midar-' in x:
            dir_midar = '/home/%s/%s/midar/midar-full' % (slice_name, x)
            ejecutar = True
            break

    if ejecutar:
        # Checking for other midar process running
        ejecutar = True
        for x in listdir('/home/%s' % slice_name):
            if x == 'MIDAR_WORKING':
                ejecutar = False
                break

        if ejecutar:
            # RUNNING MODE
            logging.info('Mode: %s' % opcion)
            # LOADING IPs TO RESOLVER
            ejecutar = False
            nombre = 'direcciones_%s' % ID
            for x in listdir('/home/%s/%s' % (slice_name, user)):
                if nombre in x:
                    dir_file = '/home/%s/%s/%s' % (slice_name, user, x)
                    ejecutar = True
                    break

            if ejecutar:
                try:
                    # FILE TO INDICATE THAT A MIDAR PROCESS IS RUNNING
                    call(['touch /home/%s/MIDAR_WORKING' % slice_name], shell=True)

                    # FINDING LOCAL GATEWAY AND INTERFACE
                    proceso = Popen(['find-gateway'], stdout=PIPE)
                    aux = proceso.stdout.read().splitlines()
                    proceso.stdout.close()

                    gateway = aux[0].split('=')[1]
                    iface = aux[1].split('=')[1]

                except:
                    logging.error('Error: Not found gateway/interface')
                else:
                    logging.info('GW/iface: %s;%s' % (gateway,iface))

                    # Generating a ping to gateway every 45s
                    call(['ping %s -i 45 >/dev/null 2>&1 &' % gateway], shell=True)

                    # Finding number of processors
                    try:
                        proceso = Popen(['nproc'], stdout=PIPE)
                        aux = proceso.stdout.read().splitlines()
                        proceso.stdout.close()
                        threads = aux[0]
                    except:
                        threads = '1'

                    try:
                        # Starting MPER
                        p = system('sudo /usr/local/bin/mper -D %s -p %s -G %s -I %s & >/dev/null 2>&1' % (mper_port, mper_pps, gateway, iface))
                        sleep(1)

                        # MIDAR command

                        comando = 'sudo %s --autostep ' % dir_midar

                        if opcion == 'estimacion':
                            comando = comando + '--stop=disc-targets '
                        elif opcion == 'final':
                            pass

                        comando = comando + """--run-id=%s --threads=%s --mper-port=%s --mper-pps=%s --targets=%s --dir=%s --est-duration=%s --est-rounds=%s --elim-rounds=%s --cor-rounds=%s --est-overlap=%s --disc-overlap=%s --elim-overlap=%s --cor-overlap=%s start""" % (
                            ID,  threads, mper_port, mper_pps, dir_file, dir_resultados, est_duration, est_rounds, elim_rounds,
                            cor_rounds, est_overlap, disc_overlap, elim_overlap, cor_overlap)

                        # Starting MIDAR
                        logging.info('START MIDAR')
                        logging.info('comando: %s' % comando)
                        call([comando], shell=True)
                        logging.info('END MIDAR')

                    except Exception, e:
                        logging.exception(e)

                    else:
                        if opcion == 'estimacion':
                            archivo_origen = '/home/%s/%s/run-%s/estimation-analysis/target-summary.txt' % (slice_name, user, ID)
                        elif opcion == 'final':
                            archivo_origen = '/home/%s/%s/run-%s/midar-analysis/midar-noconflicts.sets' % (slice_name, user, ID)

                        archivo_destino = '/home/%s/%s/resultados__%s__%s__.txt' % (slice_name, user, ID, hostname)

                        call(['sudo cp %s %s' % (archivo_origen, archivo_destino)], shell=True)

                        call(['sudo gzip -9 %s' % archivo_destino], shell=True)

                    finally:
                        # KILLING PROCESS
                        p = system('sudo pkill mper >/dev/null 2>&1')
                        p = system('sudo pkill mper >/dev/null 2>&1')
                        p = system('pkill ping >/dev/null 2>&1')
                        p = system('pkill mper >/dev/null 2>&1')

                        # REMOVING TEMPORARY FILES
                        call(['sudo rm -drf /home/%s/%s/run-%s' % (slice_name, user, ID)], shell=True)

                finally:
                    call(['sudo rm ~/MIDAR_WORKING'], shell=True)

            else:
                logging.error('IPs to resolve NOT FOUND')

        else:
            logging.info('NODE BUSY')

    else:
        logging.error('MIDAR NOT FOUND')

    # END
    logging.info('END SCRIPT')

    return None

# BEGINNING
if __name__ == '__main__':
    estado = main()
    exit(estado)

