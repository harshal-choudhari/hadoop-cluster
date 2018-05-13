#!/usr/bin/python
#Assumptions:
#   1. All Paths given in config file should be absolute


import ConfigParser, sys, os, logging, subprocess, traceback, socket

#Function for password less ssh
def passless(ip):
    os.system("echo -e 'yes\n%s' | ssh hduser@%s mkdir -p ~/.ssh"%(cpass, ip))
    os.system("echo -e '%s' | scp -r %s/.ssh/id_rsa.pub hduser@%s:~/.ssh/authorized_kyes >>/dev/null"%(cpass, MY_HOME, ip))

def installHadoop():
    #variables
    output = 'y'
    cmd  = []

    #Setting file paths
    try:
        INSTALL_PATH = config.get('paths','install_path')
        JDK_TAR_FILE = config.get('paths','jar_tar_file')
        HADOOP_TAR_FILE = config.get('paths','hadoop_tar_file')
        JAVA_HOME = config.get('paths','java_home')
        HADOOP_HOME = config.get('paths','hadoop_home')
        JAVA_DIR = config.get('paths','java_dir')
        HADOOP_DIR = config.get('paths','hadoop_dir')
        logFilePath = config.get('paths','logFilePath')
        machineIp = config.get('systemInfo','machineIp')
        machineHost = config.get('systemInfo','machineHostName')
        masterIp = config.get('masterInfo','masterIp')
        masterHost = config.get('masterInfo','masterhostName')
        ipList = (config.get('masterInfo','ipList')).split(',') #if machineType is slave then pass NA
        hostNameList  = (config.get('masterInfo','hostList')).split(',')  #if machineType is slave then pass NA
    except Exception:
        s = traceback.format_exc()
        serr = "Error description:\n%s\n" %(s)
        sys.stderr.write(serr)
        
    while True:
        #check if files are in directory
        if  os.path.isfile(JDK_TAR_FILE):
            logging.error("java tar file not in current directory")
            print "\njava tar is not in directory.\nIf file is in directory press y to proceed else press n"
            output = (raw_input().strip('\n')[0]).lower()
            if output == 'n':
                sys.exit(1)
                continue
            if os.path.isfile(HADOOP_TAR_FILE):
                logging.error("hadoop tar file not in current directory")
                print "\nhadoop tar is not in directory.\nIf file is in directory press y to proceed else press n"
                output = (raw_input().strip('\n')[0]).lower()
                if output == 'n':
                    sys.exit(1)
                    continue
                else:
                    break
                
    #Purge openJdk
    cmd.append("apt-get purge openjdk* -y >>/dev/null")
    cmd.append("mkdir %s"%(INSTALL_PATH))
    cmd.append("tar -xzf %s -C %s"%(JDK_TAR_FILE, INSTALL_PATH))
    cmd.append("tar -xzf %s -C %s"%(HADOOP_TAR_FILE,INSTALL_PATH))
    cmd.append("mv %s/jdk* %s/java"%(INSTALL_PATH,INSTALL_PATH))
    cmd.append("mv %s/hadoop* %s/hadoop"%(INSTALL_PATH,INSTALL_PATH))
    cmd.append("echo 'JAVA_HOME=%s\nPATH=$PATH:%s/bin\nexport JAVA_HOME\nexport PATH' >> /etc/profile"%(JAVA_HOME, JAVA_HOME))
    
    #Restart /etc/profile
    cmd.append(". /etc/profile")

    #Chnage ip address of interface eth0
    cmd.append("cp hadoop/interfaces /etc/network/interfaces")
    cmd.append("ifdown eth0;ifup eth0;/etc/init.d/networking stop;/etc/init.d/networking start;")
    
    cmd.append("echo -e '%s\n%s\n' | adduser --ingroup hadoop hduser"%(cpass,cpass))
    cmd.append("echo 'hduser:%s' | chpasswd"%(cpass))
    cmd.append("chown -R hduser %s"%(HADOOP_HOME))
    cmd.append("mkdir -p /usr/local/hadoop/yarn_data/hdfs/namenode")
    cmd.append("mkdir -p /usr/local/hadoop/yarn_data/hdfs/datanode")

    #Disable IPv6
    cmd.append("echo '#disable IPv6\nnet.ipv6.conf.all.disable_ipv6 = 1\nnet.ipv6.conf.default.disable_ipv6 = 1\nnet.ipv6.conf.lo.disable_ipv6 = 1' >> /etc/sysctl.conf")
    while cmd:
        try:
            subprocess.call(cmd[0],shell=True)
            subprocess.call("sleep 1",shell=True)
        except OSError:
            logging.error("Error while executing: %s"%(cmd[0]))
            break;
        cmd.remove(cmd[0])
    #if sudo does not work then add sudo -u hduser
    #following setup can be done by using function so that it will minimize the code
    subprocess.call("echo '%s\n' | sudo -u hduser mkdir -p %s/.ssh '' "%(cpass,MY_HOME),shell=True)
    subprocess.call("echo '%s\n' | sudo -u hduser ssh-keygen -t rsa -P '' "%(cpass),shell=True)
    subprocess.call("cat %s/.ssh/id_rsa.pub >> %s/.ssh/authorized_keys"%(MY_HOME,MY_HOME),shell=True)
    subprocess.call("echo '127.0.0.1\tlocalhost\t%s' > /etc/hosts"%(socket.gethostname()),shell=True)
    if(systemType == 'master'):
        for i in range(0,len(ipList)):
            subprocess.call("echo '%s\t%s' >> /etc/hosts"%(ipList[i],hostNameList[i]),shell=True)
            subprocess.call("echo '%s' >> %s/etc/hadoop/slaves"%(hostNameList[i], HADOOP_HOME),shell=True)
        for i in ipList:
            passless(i)
    else:
        subprocess.call("echo '%s\t%s\n%s\t%s' >> /etc/hosts"%(masterIp,masterHost,machineIp,machineHost),shell=True)
        for i in hostNameList:
            subprocess.call("echo '%s' >> %s/etc/hadoop/slaves"%(i, HADOOP_HOME),shell=True)
        
    #update hadoop files
    cmd.append("cp hadoop/hadoop-env.sh %s/etc/hadoop/hadoop-env.sh"%(HADOOP_HOME))
    cmd.append("cp hadoop/core-site.xml %s/etc/hadoop/core-site.xml"%(HADOOP_HOME))
    cmd.append("cp hadoop/hdfs-site.xml %s/etc/hadoop/hdfs-site.xml"%(HADOOP_HOME))
    cmd.append("cp %s/etc/hadoop/mapred-site.xml.template %s/etc/hadoop/mapred-site.xml"%(HADOOP_HOME,HADOOP_HOME))
    cmd.append("cp hadoop/mapred-site.xml %s/etc/hadoop/mapred-site.xml"%(HADOOP_HOME))
    cmd.append("cp hadoop/yarn-site.xml %s/etc/hadoop/yarn-site.xml"%(HADOOP_HOME))
    cmd.append("cp hadoop/bashrc %s/.bashrc"%(MY_HOME))
    cmd.append("sudo -u hduser %s/bin/hdfs namenode -format"%(HADOOP_HOME))
    while cmd:
        try:
            subprocess.call(cmd[0],shell=True)
            subprocess.call("sleep 1",shell=True)
        except OSError:
            logging.error("Error while executing: %s"%(cmd[0]))
            break;
        cmd.remove(cmd[0])
#Add slave 
def addSlave():
    slaveName = config.get('slaveInfo','slaveName')
    slaveIp = config.get('slaveInfo','slaveIp')
    if systemType == 'master':
        subprocess.call("echo '%s\t%s' >> /etc/hosts"%(slaveName, slaveIp),shell=True) 
        passless(slaveIp)
        subprocess.call("echo '%s\n' >> %s/etc/hadoop/slaves"%(slaveName,HADOOP_HOME),shell=True)
    else:
        logging.error("User not authorized to add slave")
#Remove slave 
def removeSlave():
    slaveName = config.get('slaveInfo','slaveName')
    slaveIp = config.get('slaveInfo','slaveIp')
    if systemType == 'master':
        subprocess.call("sed -i 's/%s/ /' /etc/hosts"%(slaveIp),shell=True)
        subprocess.call("sed -i 's/%s/ /' /etc/hosts"%(slaveName),shell=True)
        subprocess.call("sed -i 's/%s/ /' %s/hadoop/etc/hadoop/slaves"%(slaveName, HADOOP_HOME),shell=True)
    else:
        logging.error("User not authorized to add slave")
#def updateHadoop(sys.argv[1]):

#Main
try:
    config = ConfigParser.ConfigParser()
    config.read(sys.argv[1])
    operation = config.get('operation','opType')
    systemType = config.get('systemInfo','machineType') #if it is master or slave
    logFilePath = config.get('paths','logFilePath')
    os.system("mkdir %s"%(logFilePath))
    cpass = config.get('systemInfo','hduserPassword')
    MY_HOME = config.get('paths','home_dir')
except Exception:
    s = traceback.format_exc()
    serr = "Error description:\n%s\n" %(s)
    sys.stderr.write(serr)
try:
    logging.basicConfig(filename=logFilePath, format='%(levelname)s - %(asctime)s - %(message)s - %(process)d - - %(filename)s - %(module)s - %(lineno)d',level=logging.DEBUG)
except IOError as e:
    print e.strerror

if str(operation) == 'install':
    installHadoop()
#elif operation == 'update':
    #updateHadoop() 
elif str(operation) == 'addSlave':
    addSlave()
elif str(operation) == 'removeSlave':
    removeSlave()
