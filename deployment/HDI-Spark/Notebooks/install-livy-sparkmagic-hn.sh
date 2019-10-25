#!/bin/bash
#
# Update Livy Jars
#

sudo wget https://ruinliureplhdistorage.blob.core.windows.net/ruinliurepl/livy_jar.zip -O livy_jar.zip
sudo unzip livy_jar.zip

if [ "$(ls -A /usr/hdp/current/livy2-server/jars)" ]; then
     sudo rm /usr/hdp/current/livy2-server/jars/*
fi
if [ "$(ls -A /usr/hdp/current/livy2-server/repl_2.11-jars)" ]; then
     sudo rm /usr/hdp/current/livy2-server/repl_2.11-jars/*
fi
if [ "$(ls -A /usr/hdp/current/livy2-server/rsc-jars)" ]; then
     sudo rm /usr/hdp/current/livy2-server/rsc-jars/*
fi

sudo cp livy_jar/jars/* /usr/hdp/current/livy2-server/jars/
sudo cp livy_jar/repl_2.11-jars/* /usr/hdp/current/livy2-server/repl_2.11-jars/
sudo cp livy_jar/rsc-jars/* /usr/hdp/current/livy2-server/rsc-jars/

#
# Update SparkMagic
#

sudo wget https://ruinliureplhdistorage.blob.core.windows.net/ruinliurepl/config.json -O config.json
sudo cp config.json /home/spark/.sparkmagic/

sudo wget https://ruinliureplhdistorage.blob.core.windows.net/ruinliurepl/sparkmagic.zip -O sparkmagic.zip
sudo unzip sparkmagic.zip

sudo cp sparkmagic/livyclientlib/command.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/livyclientlib/command.py
sudo cp sparkmagic/livyclientlib/livysession.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/livyclientlib/livysession.py
sudo cp sparkmagic/livyclientlib/sparkstorecommand.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/livyclientlib/sparkstorecommand.py
sudo cp sparkmagic/livyclientlib/sqlquery.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/livyclientlib/sqlquery.py

if [ ! -d "/usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/kernels/sparkdotnetkernel" ]
then
    sudo mkdir /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/kernels/sparkdotnetkernel
fi
sudo cp sparkmagic/kernels/sparkdotnetkernel/* /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/kernels/sparkdotnetkernel/
# install sparkdotnetkernel
sudo /usr/bin/anaconda/bin/jupyter-kernelspec install /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/kernels/sparkdotnetkernel

sudo cp sparkmagic/magics/remotesparkmagics.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/magics/remotesparkmagics.py
sudo cp sparkmagic/magics/sparkmagicsbase.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/magics/sparkmagicsbase.py

sudo cp sparkmagic/utils/configuration.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/utils/configuration.py
sudo cp sparkmagic/utils/constants.py /usr/bin/anaconda/lib/python2.7/site-packages/sparkmagic/utils/constants.py

# Remove the temporary livy jars and sparkmagic files.
sudo rm livy_jar.zip
sudo rm -rf livy_jar
sudo rm sparkmagic.zip
sudo rm -rf sparkmagic
