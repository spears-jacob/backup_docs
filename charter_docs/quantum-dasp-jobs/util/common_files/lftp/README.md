# This jar contains an LFTP binary to use on any EMR cluster and a shell script
# for ease of using the binary.
EMR versions tested: 5.30.1, 5.33.0, 6.3.0, and 6.5.0

# to use LFTP from a precompiled binary on an EMR cluster, follow the below steps
sudo -s yum -y remove lftp
mkdir folder_lftp; cd folder_lftp;
aws s3 cp s3://pi-global-${ENVIRONMENT}-udf-jars/lftp.jar .
jar -xfM lftp.jar
sudo -s cp lftp.4.9.2.binary.EMR5.30.1 /usr/local/bin/lftp
sudo -s ln -s /usr/local/bin/lftp /usr/bin/lftp
sudo -s chmod 777 /usr/local/bin/lftp
cp lftp.2022.01.sh ../lftp.sh
cd ..
lftp --version


# to build LFTP from binary on an EMR cluster, start one and connect to it.
# then find the appropriate version from the website ftp, and follow the below steps
start_emr
sudo -s -u hadoop
cd ~
sudo -s yum -y remove lftp
wget http://lftp.yar.ru/ftp/lftp-4.9.2.tar.bz2
tar -xf lftp-4.9.2.tar.bz2
cd lftp-4.9.2
sudo -s yum -y groupinstall "Development Tools"
sudo -s yum -y install ncurses-devel
sudo -s yum -y install readline-devel
sudo -s yum -y install gnutls-devel
./configure
make
sudo -s make install
sudo -s ln -s /usr/local/bin/lftp /usr/bin/lftp
cd ..
lftp --version

# to put a jar file together:
jar cf jar-file input-file(s)
