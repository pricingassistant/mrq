#!/usr/bin/env bash

# Setup OpenSSHd
echo "Configuring OpenSSH"
mkdir -p /root/.ssh
chmod 700 /root/.ssh
echo "${AUTH_KEY}" > /root/.ssh/authorized_keys
chmod 600 /root/.ssh/*
chown -Rf root:root /root/.ssh
mkdir -p /var/run/sshd
mkdir -p /etc/ssh
mv /root/sshd_config /etc/ssh/sshd_config

apt-get -y install openssh-server