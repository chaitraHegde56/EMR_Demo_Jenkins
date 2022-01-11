#!/usr/bin/env bash

# install conda
#wget --quiet https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh \
    #&& /bin/bash ~/miniconda.sh -b -p $HOME/conda

#echo -e '\nexport PATH=$HOME/conda/bin:$PATH' >> $HOME/.bashrc && source $HOME/.bashrc

# install packages
#conda install -y ipython jupyter
echo " ########################################### "
echo " - Setup SWAP space .. "
echo " ########################################### "
sudo dd if=/dev/zero of=/mnt/swapfile bs=1M count=10240
sudo chown root:root /mnt/swapfile
sudo chmod 600 /mnt/swapfile
sudo mkswap /mnt/swapfile
sudo swapon /mnt/swapfile
sudo swapon -a

echo " ########################################### "
echo " - Setting up Jars"
echo " ########################################### "
mkdir -p /home/hadoop/jars
#aws s3 cp s3://emr-threatsync-scripts/mysql-connector-java-8.0.12.jar /home/hadoop/jars
#aws s3 cp s3://emr-threatsync-scripts/aws-java-sdk-1.7.4.jar /home/hadoop/jars
#aws s3 cp s3://emr-threatsync-scripts/hadoop-aws-2.7.3.jar /home/hadoop/jars
#aws s3 cp s3://emr-threatsync-scripts/elasticsearch-spark-20_2.11-6.2.3.jar /home/hadoop/jars

aws s3 cp s3://emr-demos/aws_env.sh /home/hadoop
aws s3 cp s3://emr-demos/auto_update_permissions.sh /home/hadoop

cd "/home/hadoop"

echo " ########################################### "
echo " - Running the auto_update_permissions script .. "
echo " ########################################### "
bash auto_update_permissions.sh --add-rule
