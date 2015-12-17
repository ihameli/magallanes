## MAGALLANES
_**Large scale Internet explorer tool**_
_____________________________________________________________________
Software license: Academic Free License (AFL) [see afl-3.0.txt]

Manifest: [see manifest.sha1sum]

http://cnet.fi.uba.ar/

**Developers:**
  * Mauricio Anderson Ricci (b)
  * J. Ignacio Alvarez-Hamelin (a,b)
	   
  (a) CONICET, Argentina

  (b) Facultad de Ingeniería, Universidad de Buenos Aires, Argentina (http://www.fi.uba.ar)

_____________________________________________________________________

**Index:**
  1. Brief description of Magallanes
  2. Requirements and Installation
  3. Using Magallanes
  4. Reading results

###  1. Brief description of Magallanes

Magallanes is a tool created to run extensive explorations on Internet using a PlanetLab Slice in which, through its nodes, is possible to make traceroutes, ping and the alias resolution from many monitors distributed along the world and then collect the results and stored them in a central database.

The goal is to help scientist to collect data in an easy way to their works, so that they can saving time in collecting data.

We provide:
  * Basic options to manage a PlanetLab Slice
  * Exploration process with several options to set the traceroutes
  * Alias resolution process

###  2. Requirements and Installation

First of all you need to have a PlanetLab Slice.

Then you have to install:
  * Python 2.7, and libraries:
    * xmlrpclib
    * json
    * pexpect
    * psycopg2 (https://pypi.python.org/pypi/psycopg2)
    * netaddr (https://pypi.python.org/pypi/netaddr)
    * geoip2 (http://geoip2.readthedocs.org/en/latest/)
  * Postgresql 9.4 or greater 
  * parallel-ssh
  * sc_warts2json (This program is installed with Scamper.
  	http://www.caida.org/tools/measurement/scamper/)

And download MaxMind GeoLite2 Free Databases:
  * http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz
  * http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country-CSV.zip

From the previous downloads you have to obtain the next files and put them into _/home/.../magallanes/src/files_ folder:
  * GeoLite2-Country-Blocks-IPv4.csv
  * GeoLite2-Country-Locations-en.csv
  * GeoLite2-City.mmdb
    
Next step is prepare the database. For this you have to create a new empty database on Postgresql and set their parameters in the config file: _home/.../magallanes/src/files/program.conf_.

The other parameters to set in the config file are:
  * ssh_time_out:  Time out in the ssh conexion. Recommended 45.
  * publicKeyRSA: The path to yout public Key.
  * ssh_pass: Your ssh password.
  * num_thread: Num of parallel thread to comunicate to nodes. Recommended 15.
  * midar_folder: The folder which is obtained after uncompress midar. For example _midar-0.5.0_.
  * num_ip_list: max number of IP to resolver by node in alias resolution process. Recommended 35000. Max 40000. 

Then you have to prepare the _filesToInstall.tar.gz_ which will cointain the programs to install in the remote nodes. For this, first download:
  * scamper (http://www.caida.org/tools/measurement/scamper/)
  * midar (http://www.caida.org/tools/measurement/midar/)
  * mper (https://www.caida.org/tools/measurement/mper/)
  * rb-perio (http://www.caida.org/tools/measurement/rb-mperio/)
  * arkutil (http://www.caida.org/tools/utilities/arkutil/)
 
Once you downloaded the previous programs, add them to the _filesToInstall.tar.gz_, and you will have to obtain the next files in it: 'zlib-devel-1.2.5-2.fc14.i686.rpm', 'scamper-cvs-20XXXXXX.tar.gz', 'rb-mperio-X.X.X.gem', 'mper-X.X.X.tar.gz', 'midar-X.X.X.tar.gz', 'arkutil-X.X.X.gem', 'fedora-updates-testing.repo', 'fedora-updates.repo', 'fedora.repo'.

Next step is check in _/home/.../magallanes/src/installNodes.sh_ if the _Varibles_ are correct regarding the sofware version in the _filesToInstall.tar.gz_.

Finally, you could edit your _/home/user/.bashrc_ file to add the next line: 

  > alias magallanes='python /home/.../magallanes/src/main.py' 

After that, you could start the program just typing:

  > magallanes

###  3. Using Magallanes

After you start Magallanes, the first screen is the _User menu_ from where you have three options:

  1. Users: Access with an stored user
  2. Manual: Access manually typing the username and password of PlanetLab 
  3. Options: Add/Remove stored users to access directly without the necessity of entry your names/pass all the time
 
Then, once you enter the user, it will appear the slices available. You have to choose the one you want to work with. The program will continue checking the conexion to remote nodes and finally it will present the _Main menu_, in which it is possible to access to the two main sections: 1. Administration of nodes; 2. Administration of explorations.

  1. **Admin nodes**
    1. **View nodes**: Show info of all the nodes in the Slice.
    2. **Add nodes**: Add nodes into the Slice. The delay between from the moment in which the node is added to the slice until it is available it could be long.
    3. **Install software in nodes**: Install software in nodes. Required to use them for running explorations.
    4. **Remove nodes**: Remove nodes from the Slice. 
    5. **Update information of Planetlab nodes**: Update the _planetlab-nodes_ table with the data of all the nodes in the platform.
    6. **Update table of ipv4 address blocks**: Update the _address-block-ipv4_ table with the info content in the MaxMind database files.

  2. **Admin explorations**
    1. **View functional nodes**: Show nodes which are ready to use in an exploration, ie with ssh connexion and the necessary software installed.
    2. **Summery of last explorations**: Brief summary of the last executed exploration. Info obtained from _analysis_ table.
    3. **New exploration**: Configure and start a new exploration.
    4. **Cancel exploration**: Stop an exploration in execution and remove all residual files in the nodes.
    5. **Stored exploration**: Download results of an exploration and store it in the database.
    6. **Alias resolution**: Configure and start a new alias resolution process.
    7. **Delete data**: Delete info from the database related to an exploration.

When an exploration is programed (**New exploration**), there are a lot of option to set it:

  1. **Nodes**: To select the nodes which the exploration will use as monitors.

  2. **Kind of target**: From where, and the criteria, the target will be elected.

    To run an exhaustive exploration, the appropriate option is _"3. Internet; Random"_ due to it allow select many IPs distributed along the world. The criteria to select the destinies consists in take an IP from a random IP block obtained from the MaxMind database. Once an IP is selected from a block, such block will not be elected again until the next round, ie when it would be elected one random IP from each blocks.

  3. **Period of traceroutes**: Set the the minimum time between a round of traceroutes start and the next one.
  
  4. **Duration of exploration**: Set the duration of the exploration.
  
  5. **Probe type**: Specifies the traceroute method to use. The probe available are: UDP, ICMP, UDP-paris, ICMP-paris, TCP, and TCP-ACK.
  
  6. **PPS**: Specifies the target packets-per-second rate for scamper to reach.
  
  7. **GATLIMIT**: Specifies the number of unresponsive hops permitted until a check is made to see if the destination will respond.
  
  8. **WAIT**: Specifies how long to wait, in seconds, for a reply.
  
  9. **Recalculatino algorithm**: To active or not the recalculation algorithm to set the initial TTL to each destiny.
  
  10. **Series of ping**: To select if at the end of the exploration make "n" ping to all IP found previously.
  
  11. **Description**: To add a brief reminder.
  
###  4. Reading results

The output data is stored in the database indicated in the _program.conf_ file. In this database there will be several tables, many of which will be to internal purposes. The important tables with the results of the exploration are:

  * **analysis**: Root table, where there is global information about explorations
  * **traceroutes**: Traceroute headers executed in explorations
  * **hops**: Results of traceroutes
  * **ping**: Results of ping
  * **alias_resolution**: Global information about alias resolution processes
  * **ip_found**: IP found in an exploration
  * **links_ip**: Links at IP level found in an exploration
  * **links**: Links at router level found in an exploration after alias resolution
  * **interfaces**: Interfaces for each router  after alias resolution
  * **routers**: routers discovered after alias resolution

All results stored in the database were not processed, so that the user could run his own script directly on the results as if he had executed the manually exploitation. 
_____________________________________________________________________
### References

[1] PlanetLab: An open platform for developing, deploying and accessing planetary-scale services.
https://www.planet-lab.org/.

[2] CAIDA: Center for Applied Internet Data Analysis. http://www.caida.org/.

[3] MaxMind. https://www.maxmind.com/es/home.
