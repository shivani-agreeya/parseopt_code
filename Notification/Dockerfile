FROM adoptopenjdk:8-jre

RUN mkdir /opt/app

ARG JAR_FILE=target/*.jar

COPY ${JAR_FILE} /opt/app/ROOT.jar

COPY src/main/resources/ /etc/opt/notification-service/config/

ENTRYPOINT ["java", "-Dspring.config.location=/etc/opt/notification-service/config/", "-jar", "/opt/app/ROOT.jar"]
