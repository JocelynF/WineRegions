FROM python:3.8.3-buster

ADD ./* /
RUN dpkg-reconfigure debconf -f noninteractive -p critical && apt update -y
RUN sed '/st_mysql_options options;/a unsigned int reconnect;' /usr/include/mysql/mysql.h -i.bkp
RUN apt install lsb-release -y &&  apt install build-essential -y && apt
install nano -y && apt install emacs && apt clean


RUN pip install -r requirements.txt
