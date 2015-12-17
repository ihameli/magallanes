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

import pexpect
from sys import argv

# # INPUT PARAMETERS
slice_name = argv[1]
direccion_clave_publica = argv[2]
clave_ssh = argv[3]
nodo = argv[4]

password = "Enter passphrase for key '" + direccion_clave_publica + "'"
ssh_newkey = 'Are you sure you want to continue connecting (yes/no)?'

def main():
    """ Script para conceder permiso sudo en los nodos fedora 14 """

    p = pexpect.spawn('ssh -l ' + slice_name + ' -i ' + direccion_clave_publica + ' ' + nodo)
    i = p.expect([ssh_newkey,password,".*password.*",pexpect.EOF, pexpect.TIMEOUT])

    if i == 0:
        # NESTED NODE a known host
        p.sendline('yes')
        i = p.expect([ssh_newkey,password,pexpect.EOF])

    if i == 1 or i == 2:
        # ACCESSING TO NODE
        p.sendline(clave_ssh)
        a = p.expect(["Last login:.*","[.* ~]$",pexpect.EOF, pexpect.TIMEOUT])

        if a == 0 or a == 1:
            a = p.expect(["[.* ~]$",pexpect.EOF, pexpect.TIMEOUT])
            if a == 0:
                # ACCESSING TO ROOT MODE
                p.sendline("su")
                p.expect(".*#")
                # CHANGE TO SUDOERS
                p.sendline("echo 'Defaults visiblepw' >> /etc/sudoers")
                p.expect(".*#")
                # LEAVING NODE
                p.sendline("exit")
                p.sendline("\x03")
            else:
                pass

        elif a==3 or a==4:
            pass

    # I either got key or connection timeout
    elif i == 3 or i == 4:
        pass

    return None

if __name__ == '__main__':
    estado = main()
    exit(estado)
