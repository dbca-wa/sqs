# Prepare the base environment.
FROM dbcawa/sqs:latest as builder_base_sqsdev
USER root
RUN apt-get update
RUN apt-get install --no-install-recommends -y npm patch
RUN apt-get install --no-install-recommends -y libpq-dev git ssh sudo 
RUN apt-get install --no-install-recommends -y screen
RUN apt-get install --no-install-recommends -y virtualenv
RUN apt-get install --no-install-recommends -y python3-dev
RUN openssl req -new -newkey rsa:4096 -days 3650 -nodes -x509 -subj  "/C=AU/ST=Western Australia/L=Perth/O=Development Environment/OU=IT Department/CN=example.com"  -keyout /etc/ssl/private/selfsignedssl.key -out /etc/ssl/private/selfsignedssl.crt

RUN echo 'if ! [ -n "${SUDO_USER}" -a -n "${SUDO_PS1}" ]; then' >> /etc/bash.bashrc
# RUN echo "export PS1='\e[0;31mdev:\e[m \e[0;32m\${debian_chroot:+(\$debian_chroot)}\u@\h:\w\$\e[m '" >> /etc/bash.bashrc
# fix the line carriage issue
RUN echo "export PS1='\[\e[;31m\]dev:\[\e[m\] \[\e[;32m\]\u@\h:\w \$ \[\e[m\]'" >> /etc/bash.bashrc
RUN echo "fi" >> /etc/bash.bashrc


# For Linux
RUN groupadd -g 20000 container
RUN useradd -g 20000 -u 200000 container -s /bin/bash

RUN mkdir /home/container
RUN chown container.container /home/container
RUN cp /root/.bashrc /home/container
RUN cp /root/.profile /home/container



EXPOSE 8080
HEALTHCHECK --interval=1m --timeout=5s --start-period=10s --retries=3 CMD ["wget", "-q", "-O", "-", "http://localhost:8080/"]
CMD ["/startup.sh"]
