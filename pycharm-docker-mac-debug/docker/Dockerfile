FROM mrq_local:latest

RUN apt-get update

COPY pycharm-docker-mac-debug/docker/files/sshd_config  pycharm-docker-mac-debug/docker/files/ssh-install.sh /root/

RUN chmod +x /root/ssh-install.sh

# SSHD key
ARG AUTH_KEY

RUN /root/ssh-install.sh
