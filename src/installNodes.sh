#!/bin/bash 

# Varibles
SCAMPER='scamper-cvs-20141101.tar.gz'
SCAMPER_FOLDER='scamper-cvs-20141101'
MIDAR='midar-0.5.0.tar.gz'
MIDAR_FOLDER='midar-0.5.0'
MPER='mper-0.4.1.tar.gz'
MPER_FOLDER='mper-0.4.1'
RBMPERIO='rb-mperio-0.3.3.gem'
ARKUTIL='arkutil-0.13.5.gem'
ZLIB='zlib-devel-1.2.5-2.fc14.i686.rpm'

# Deleting previous files
sudo rm $SCAMPER
sudo rm $RBMPERIO
sudo rm $ARKUTIL
sudo rm $MPER
sudo rm $MIDAR
sudo rm $ZLIB
sudo rm instalacion.log fedora.repo fedora-updates.repo fedora-updates-testing.repo  
sudo rm -drf $SCAMPER_FOLDER
sudo rm -drf $MPER_FOLDER

# BEGINNING
echo "BEGIN" > instalacion.log
date >> instalacion.log

fedora_14="Fedora release 14 (Laughlin)"

if [ -f /etc/fedora-release ];
then
	cat /etc/fedora-release >> instalacion.log
	distro=$(cat /etc/fedora-release)
else
	echo "fedora-release NOT FOUND" >> instalacion.log
	distro="NOT_FOUND"
fi

# UPDATING REPOSITORIES
echo "UPDATING REPOSITORIES" >> instalacion.log
tar zxvf ~/filesToInstall.tar.gz fedora.repo fedora-updates.repo fedora-updates-testing.repo
sudo mv /etc/yum.repos.d/fedora.repo /etc/yum.repos.d/fedora.repo.old
sudo mv /etc/yum.repos.d/fedora-updates.repo /etc/yum.repos.d/fedora-updates.repo.old
sudo mv /etc/yum.repos.d/fedora-updates-testing.repo /etc/yum.repos.d/fedora-updates-testing.repo.old
sudo mv ~/fedora.repo /etc/yum.repos.d/
sudo mv ~/fedora-updates.repo /etc/yum.repos.d/
sudo mv ~/fedora-updates-testing.repo /etc/yum.repos.d/
if [ -f ~/fedora.repo /etc/yum.repos.d/ ] || [ -f ~/fedora-updates.repo ] || [ -f ~/fedora-updates-testing.repo ]; then echo "  FAILED" >> instalacion.log; else echo "  CORRECT" >> instalacion.log; fi;

# BASIC PACKAGES
echo "BASIC PACKAGES" >> instalacion.log
sudo yum install -y --nogpgcheck gcc gcc-c++ make at 
if [ -f /usr/bin/gcc ] && [ -f /usr/bin/make ] && [ -f /usr/bin/at ]; then echo "  CORRECT" >> instalacion.log; else echo "  FAILED" >> instalacion.log; fi;

# SCAMPER
echo "SCAMPER" >> instalacion.log
if [ -f /usr/local/bin/scamper ];
then
	echo "  SCAMPER: CORRECT" >> instalacion.log
else
	tar zxvf ~/filesToInstall.tar.gz $SCAMPER
	tar zxvf ~/$SCAMPER
	cd ~/$SCAMPER_FOLDER/
	sudo ./configure
	sudo make
	sudo make install
	cd ~/
	if [ -f /usr/local/bin/scamper ];
	then
		echo "  SCAMPER: CORRECT" >> instalacion.log
		sudo rm ~/$SCAMPER
		sudo rm -drf ~/$SCAMPER_FOLDER
	else
		echo "  SCAMPER: FAILED" >> instalacion.log
	fi
fi

# IF Fedora 14 >> INTALL MIDAR
echo "MIDAR" >> instalacion.log
if [ "$distro" = "$fedora_14" ];
then
	# RUBY
	sudo yum install -y --nogpgcheck ruby rubygems ruby-devel
	if [ -f /usr/bin/ruby ]; then echo "  RUBY: CORRECT" >> instalacion.log; else echo "  RUBY: FAILED" >> instalacion.log; fi;

	echo "  GEMS" >> instalacion.log
	tar zxvf ~/filesToInstall.tar.gz $RBMPERIO $ARKUTIL
	sudo gem install $RBMPERIO	
	sudo gem install $ARKUTIL
	sudo rm $ARKUTIL $RBMPERIO

	# GZopen
	tar zxvf ~/filesToInstall.tar.gz $ZLIB
	sudo yum install -y --nogpgcheck $ZLIB
	sudo rm $ZLIB

	# MPER
	tar zxvf ~/filesToInstall.tar.gz $MPER
	tar xzvf ~/$MPER
	cd ~/$MPER_FOLDER/
	sudo ./configure
	sudo make
	sudo make install
	cd ~/
	if [ -f /usr/local/bin/mper ];
	then	
		echo "  MPER: CORRECT" >> instalacion.log
		sudo rm ~/$MPER
		sudo rm -drf ~/$MPER_FOLDER
	else
		echo "  MPER: FAILED" >> instalacion.log
	fi

	# MIDAR
	tar zxvf ~/filesToInstall.tar.gz $MIDAR
	tar xzvf ~/$MIDAR
	cd ~/$MIDAR_FOLDER/
	sudo ./configure
	sudo make
	cd ~/
	if [ -f ~/$MIDAR_FOLDER/midar/midar-full ];
	then	
		echo "  MIDAR: CORRECT" >> instalacion.log
		sudo rm ~/$MIDAR
	else
		echo "  MIDAR: FAILED" >> instalacion.log
	fi
else
	echo "  IS NOT FEDORA 14" >> instalacion.log
fi

# REMOVING INTALL FILES
echo "END" >> instalacion.log
date >> instalacion.log

sudo rm ~/installNodes.sh 
sudo rm ~/filesToInstall.tar.gz

