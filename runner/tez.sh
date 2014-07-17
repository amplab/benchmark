sudo yum -y groupinstall "Development Tools"
cd /tmp
wget http://apache.mesi.com.ar/incubator/tez/tez-0.2.0-incubating/tez-0.2.0-incubating.tar.gz
wget http://www.motorlogy.com/apache/maven/maven-3/3.1.1/binaries/apache-maven-3.1.1-bin.tar.gz
wget https://protobuf.googlecode.com/files/protobuf-2.5.0.tar.gz
tar zxf apache-maven-3.1.1-bin.tar.gz
tar zxf tez-0.2.0-incubating.tar.gz
tar zxf protobuf-2.5.0.tar.gz
cd protobuf-2.5.0
./configure
make -j8
make install
cd ../tez-0.2.0-incubating
JAVA_HOME="/usr/jdk64/jdk1.6.0_31" /tmp/apache-maven-3.1.1/bin/mvn clean install -DskipTests=true -Dmaven.javadoc.skip=true

HADOOP_USER_NAME=hdfs hadoop dfs -put tez-dist/target/tez-0.2.0-full/tez-0.2.0-full /apps/

cd ..
echo "<configuration><property><name>tez.lib.uris</name><value>"\${fs.default.name}/apps/tez-0.2.0-full,\${fs.default.name}/apps/tez-0.2.0-full/lib/"</value></property></configuration>" > tez-site.xml
chmod -R 777 tez-0.2.0-incubating

