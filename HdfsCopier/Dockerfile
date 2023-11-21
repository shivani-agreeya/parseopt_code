FROM amazoncorretto:8u312

ARG JAR_FILE=target/*.jar

COPY ${JAR_FILE} app/ROOT.jar

COPY src/main/resources/ /etc/opt/hdfs-copier/config/

ENTRYPOINT ["java", "-Dspring.config.location=/etc/opt/hdfs-copier/config/", "-jar","app/ROOT.jar"]
