#!/bin/bash
#
# Update Livy Jars
#

sudo wget https://ruinliureplhdistorage.blob.core.windows.net/ruinliurepl/livy_jar.zip
sudo unzip livy_jar.zip

rm -f /usr/hdp/current/livy2-server/jars/*
rm -f /usr/hdp/current/livy2-server/repl_2.11-jars/*
rm -f /usr/hdp/current/livy2-server/rsc-jars/*

sudo cp livy_jar/* /usr/hdp/current/livy2-server/

#
# Update SparkMagic
#

sudo wget https://ruinliureplhdistorage.blob.core.windows.net/ruinliurepl/sparkmagic.zip
sudo unzip sparkmagic.zip

sudo cp sparkmagic/config.json /home/spark/.sparkmagic/

sudo cp sparkmagic/livyclientlib/command.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/livyclientlib/command.py
sudo cp sparkmagic/livyclientlib/livysession.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/livyclientlib/livysession.py
sudo cp sparkmagic/livyclientlib/sparkstorecommand.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/livyclientlib/sparkstorecommand.py
sudo cp sparkmagic/livyclientlib/sqlquery.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/livyclientlib/sqlquery.py

if [ ! -d "/usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/kernels/sparkdotnetkernel" ]
then
    sudo mkdir /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/kernels/sparkdotnetkernel
fi
sudo cp sparkmagic/* /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/

# install sparkdotnetkernel
sudo /usr/bin/anaconda/bin/jupyter-kernelspec install /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/kernels/sparkdotnetkernel

#
# Remove the temporary livy jars and sparkmagic files.
#

sudo rm livy_jar.zip
sudo rm -rf livy_jar
sudo rm sparkmagic.zip
sudo rm -rf sparkmagic
