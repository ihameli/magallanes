
import subprocess
import MySQLdb as mdb
from time import strftime, sleep, time
import socket
from sys import argv
import logging

# INPUT PARAMETERS
n_hops        = argv[1]
IP_destino    = argv[2]
duracionTotal = int(argv[3])
slice_name    = argv[4]

# CLASSES DEFINITION
class TraceroutePamplona:
    def __init__(self,path,ip,n_hops,probe_type=1):
        cmd_raw="%s/newtraceroute %s -t %s -q 2 -m %s" % (path, ip, probe_type, n_hops)
        self.cmd_subprocess=cmd_raw.split()
        self.ip_dst=ip
        self.ip_local=socket.gethostbyname(socket.gethostname())
        self.id_traceroute=1

    def ParseRawResult(self,r):
        if len(r)>1:
            u_hop=0
            hops=[]
            generalidades=[]
            r_lines=r.split('\n')
            if len(r_lines)>1:
                IPstring=""
                for i in range(0,len(r_lines)-1):
                    r_col=r_lines[i].split('\t')
                    if r_col[1]!='*':
                        #IPstring+="%s."%r_col[1]
                        IPstring+="%s|"%r_col[1]
                        hops.append((self.id_traceroute,r_col[0],r_col[1],r_col[2],r_col[3],r_col[4],r_col[5]))
                        u_hop=int(r_col[0])
                        t=r_col[7]
                    else:
                        #IPstring+="X.X.X.X."
                        IPstring+="X.X.X.X|"
                        u_hop+=1
                        t=int(time())
                        hops.append((self.id_traceroute,u_hop,'X.X.X.X',0,0,0,0))

                generalidades.append((self.id_traceroute,self.ip_dst,self.ip_local,t,IPstring))

                self.id_traceroute+=1
                return hops,generalidades
            else:
                return -1
        else:
            return -1

    def execute(self):
        process = subprocess.Popen(self.cmd_subprocess, stdout=subprocess.PIPE)
        process.wait()
        output = process.communicate()[0]
        proc_output=self.ParseRawResult(output)
        return proc_output


class Pamplona2MySQL:

    def __init__(self,user,passwd,db,n_hops):
        self.con = mdb.connect('localhost', user, passwd, db);
        self.cur = self.con.cursor()

        self.cur.execute('\
                            CREATE TABLE IF NOT EXISTS traceroute_general(\
                            trace_id int primary key,\
                            dst varchar(15),\
                            src varchar(15),\
                            ts_epoch int UNSIGNED,\
                            path varchar(%s))'%(16*int(n_hops)))


        self.cur.execute('\
                            CREATE TABLE IF NOT EXISTS traceroute_hops(\
                            trace_id int,\
                            hop_n tinyint,\
                            hop_ip varchar(15),\
                            rtt float,\
                            icmp_type tinyint,\
                            icmp_code tinyint,\
                            reply_ttl tinyint UNSIGNED,\
                            foreign key (trace_id) references traceroute_general (trace_id));')


        self.cur.execute('select max(trace_id) from traceroute_general')
        self.M=self.cur.fetchone()[0]
        if self.M==None:
            self.M=0


    def Guardar(self,data1,data2):

        self.cur.executemany('\
                            INSERT INTO \
                            traceroute_general (\
                            trace_id,\
                            dst,\
                            src,\
                            ts_epoch,\
                            path) VALUES (%s,%s,%s,%s,%s)',[(str(T[0]+self.M),T[1],T[2],T[3],T[4]) for T in data2])

        #self.con.commit()

        self.cur.executemany('\
                            INSERT INTO \
                            traceroute_hops (\
                            trace_id,\
                            hop_n,\
                            hop_ip,\
                            rtt,\
                            icmp_type,\
                            icmp_code,\
                            reply_ttl) \
                            VALUES \
                            (%s,%s,%s,%s,%s,%s,%s)',\
                            [(str(T[0]+self.M),T[1],T[2],T[3],T[4],T[5],T[6]) for T in data1])
        self.con.commit()

# MAIN
def main():
    """ """

    inicio_0 = int(time())
    ejecucion = True
    primera_vuelta = True

    resultados   = "/home/%s/resultado_final.warts" % slice_name
    archivo_temp = "/home/%s/resultado_temp.warts" % slice_name

    # PREPARING LOG FILE
    logging.basicConfig(filename='/home/%s/pamplona/pamplona.log' % (slice_name),level=logging.INFO, format='%(asctime)s, %(levelname)s, %(funcName)s, %(message)s',  datefmt='%Y/%m/%d %H:%M:%S')
    logging.info('BEGIN')

    # Trucate tables
    command = 'mysql -u root -e "use Pamplona; truncate traceroute_general"; mysql -u root -e "use Pamplona; truncate traceroute_hops"'
    proceso = subprocess.Popen([command], shell=True, stdout=subprocess.PIPE)
    salidaConsola = proceso.stdout.readlines()
    proceso.stdout.close()

    if 'ERROR 2002' in salidaConsola:
        logging.error('Execution: FAILED. MySQL is not running')
        subprocess.call(['echo FAILED > resultados_general.csv'], shell=True)
        subprocess.call(['echo FAILED > resultados_hops.csv'], shell=True)
        subprocess.call(['sudo tar -czvf resultados.tar.gz resultados_general.csv resultados_hops.csv'], shell=True)

    else:
        while True:
            try:
                t = TraceroutePamplona('/home/'+slice_name+'/pamplona',IP_destino,n_hops,3)
                db = Pamplona2MySQL('root','','Pamplona',n_hops)

                inicio = int(time())
                duracion = 600
                periodo = 1

                while True:

                    inicio_trace = int(time())
                    r, p = t.execute()
                    # print r, p
                    if r != (-1):
                        db.Guardar(r, p)

                    if primera_vuelta:
                        comando = 'sudo /usr/local/bin/scamper -O planetlab -O warts -o %s -p 800 -I "trace -q 2 -w 3 -m 4 %s"' % (resultados, IP_destino)
                        subprocess.call([comando], shell=True)
                        primera_vuelta = False
                    else:
                        comando = 'sudo /usr/local/bin/scamper -O planetlab -O warts -o %s -p 800 -I "trace -q 2 -w 3 -m 4 %s"' % (archivo_temp, IP_destino)
                        subprocess.call([comando], shell=True)
                        comando = 'sudo /usr/local/bin/sc_wartscat -o %s %s' % (resultados, archivo_temp)
                        subprocess.call([comando], shell=True)
                                            
                    ahora = int(time())
                    if ahora - inicio > duracion:
                        break
                    elif (ahora - inicio_trace) < periodo:
                        sleep(periodo - (ahora - inicio_trace))
                        
            except Exception, e:
                logging.exception(e)
                ejecucion = False

            else:
                ahora = int(time())
                if ahora - inicio_0 > duracionTotal:
                    break
        try:
            if ejecucion:
                logging.info('Execution: SUCCESS')
                # get registers
                #subprocess.call(["""mysql -u root -e "use Pamplona; select * from traceroute_general as a join traceroute_hops as b on a.trace_id=b.trace_id where hop_ip<>'X.X.X.X' order by b.trace_id, b.hop_n" > resultados.csv"""], shell=True)
                subprocess.call(["""mysql -u root -e "use Pamplona; select trace_id, src, dst, ts_epoch, path from traceroute_general order by trace_id" > resultados_general.csv"""], shell=True)
                subprocess.call(["""mysql -u root -e "use Pamplona; select trace_id, hop_n, hop_ip, rtt, reply_ttl from traceroute_hops where hop_ip<>'X.X.X.X' order by trace_id, hop_n" > resultados_hops.csv"""], shell=True)
            else:
                logging.info('Execution: FAILED')
                subprocess.call(['echo FAILED > resultados_general.csv'], shell=True)
                subprocess.call(['echo FAILED > resultados_hops.csv'], shell=True)

            subprocess.call(['sudo tar -czvf resultados.tar.gz resultados_general.csv resultados_hops.csv'], shell=True)

        except Exception, e:
            logging.exception(e)
            ejecucion = False

    subprocess.call(['sudo rm resultados_general.csv resultados_hops.csv'], shell=True)
    logging.info('END')

if __name__ == '__main__':
    estado = main()
    exit(estado)
