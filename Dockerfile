FROM openjdk:8

# MAINTAINER Tejinder

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
 && curl -fsSL -o /tmp/apache-maven.tar.gz https://archive.apache.org/dist/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz \
 && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
 && rm -f /tmp/apache-maven.tar.gz \
 && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

#RUN apt-get install python3
#RUN  ln -s /usr/bin/python2 /usr/bin/python
RUN apt-get update; \
    apt-get install -y --no-install-recommends \
    python mksh

# Define working directory.
WORKDIR  /usr/local/YCSB


#RUN apt install maven -y
#RUN git clone https://github.com/brianfrankcooper/YCSB
#RUN cd YCSB && mvn clean package
#RUN cd YCSB && mvn -pl site.ycsb:mongodb-binding -am clean package

RUN git clone https://github.com/Tejinder/YCSB.git
RUN cd YCSB && mvn -pl site.ycsb:c8db-binding -am clean package

WORKDIR  /usr/local/YCSB/YCSB

#ENV YCSB_HOME /usr/local/YCSB
#RUN mkdir -p "$YCSB_HOME"
#ADD . $YCSB_HOME
#RUN cd $YCSB_HOME/YCSB && mvn -pl site.ycsb:c8db-binding -am clean package

#CMD ["cd YCSB"]
ENTRYPOINT ["./bin/ycsb"] 
#CMD ["cd YCSB"]
#CMD ["/bin/ycsb"]
#ENTRYPOINT ["/usr/local/YCSB/YCSB/bin/ycsb"]
#CMD ["--help"]
#RUN rm /bin/sh && ln -s /bin/bash /bin/sh
#SHELL ["/bin/bash", "-c"]
#RUN echo I am now using bash!
