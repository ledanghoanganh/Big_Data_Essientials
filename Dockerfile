FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk openssh-server openssh-client python3 python3-pip \
    iputils-ping iproute2 net-tools nano curl sudo \
    && rm -rf /var/lib/apt/lists/*

# User chung cho toàn cụm
ARG HADOOP_USER=hadoopanh24162006
RUN useradd -m -s /bin/bash ${HADOOP_USER} && \
    echo "${HADOOP_USER}:hadoopanh24162006" | chpasswd && \
    usermod -aG sudo ${HADOOP_USER} && \
    echo "${HADOOP_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Cấu hình SSH (sử dụng port 2222)
RUN mkdir -p /run/sshd && \
    su - ${HADOOP_USER} -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 600 ~/.ssh/authorized_keys" && \
    echo "Port 2222" >> /etc/ssh/sshd_config && \
    echo "    Port 2222" >> /etc/ssh/ssh_config && \
    echo "    StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "    UserKnownHostsFile /dev/null" >> /etc/ssh/ssh_config

# Bung bộ cài Offline từ Host vào Container
ADD --chown=${HADOOP_USER}:${HADOOP_USER} offline_pkgs/hadoop-3.5.0.tar.gz /home/${HADOOP_USER}/
ADD --chown=${HADOOP_USER}:${HADOOP_USER} offline_pkgs/spark-4.2.0-bin-hadoop3.gz /home/${HADOOP_USER}/
ADD --chown=${HADOOP_USER}:${HADOOP_USER} offline_pkgs/apache-hive-4.1.0-bin.tar.gz /home/${HADOOP_USER}/

WORKDIR /home/${HADOOP_USER}
RUN mv hadoop-3.5.0 hadoop && \
    mv spark-4.2.0-bin-hadoop3 spark && \
    mv apache-hive-4.1.0-bin hive && \
    mkdir -p hadoop/hdfs/namenode hadoop/hdfs/datanode hadoop/nm-local-dir hadoop/logs && \
    chown -R ${HADOOP_USER}:${HADOOP_USER} /home/${HADOOP_USER}

# Bổ sung thư viện commons-collections cho Hive
COPY --chown=${HADOOP_USER}:${HADOOP_USER} offline_pkgs/commons-collections-3.2.2.jar /home/${HADOOP_USER}/hive/lib/

# Khai báo biến môi trường hệ thống
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/home/${HADOOP_USER}/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV HADOOP_MAPRED_HOME=${HADOOP_HOME}
ENV HADOOP_COMMON_HOME=${HADOOP_HOME}
ENV HADOOP_HDFS_HOME=${HADOOP_HOME}
ENV HADOOP_YARN_HOME=${HADOOP_HOME}
ENV HIVE_HOME=/home/${HADOOP_USER}/hive
ENV SPARK_HOME=/home/${HADOOP_USER}/spark
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV PATH=$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${HIVE_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Tập trung biến môi trường vào /etc/profile.d/hadoop.sh và nạp cho cả interactive / non-interactive shell
RUN printf '%s\n' \
    "export JAVA_HOME=${JAVA_HOME}" \
    "export HADOOP_HOME=${HADOOP_HOME}" \
    "export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}" \
    "export HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME}" \
    "export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME}" \
    "export HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME}" \
    "export HADOOP_YARN_HOME=${HADOOP_YARN_HOME}" \
    "export HIVE_HOME=${HIVE_HOME}" \
    "export SPARK_HOME=${SPARK_HOME}" \
    "export PYSPARK_PYTHON=/usr/bin/python3" \
    "export PYSPARK_DRIVER_PYTHON=/usr/bin/python3" \
    "export CLASSPATH=\$CLASSPATH:\$HADOOP_HOME/lib/*:\$HIVE_HOME/lib/*" \
    "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HIVE_HOME/bin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" \
    > /etc/profile.d/hadoop.sh && \
    chmod +x /etc/profile.d/hadoop.sh && \
    echo "source /etc/profile.d/hadoop.sh" >> /home/${HADOOP_USER}/.bashrc && \
    echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> ${HIVE_HOME}/conf/hive-env.sh

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
