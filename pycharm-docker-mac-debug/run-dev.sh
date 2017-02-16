#!/bin/bash

if [ ! -f .gitignore ] ; then
    echo "Run me from the repository root please"
    exit 1
fi

mkdir -p pycharm-docker-mac-debug/ssh_keys # add to .gitignore!!

if [ ! -f pycharm-docker-mac-debug/ssh_keys/docker ] ; then
    echo "generating key"
    rm -f pycharm-docker-mac-debug/ssh_keys/docker*
    ssh-keygen -t rsa -f pycharm-docker-mac-debug/ssh_keys/docker -N ""
else
    echo "using existing key"
fi

# unfortunately docker native doesn't mount in /Applications, either change that or do this
rm -rf ~/.pycharm_helpers
cp -R /Applications/PyCharm.app/Contents/helpers ~/.pycharm_helpers

docker build -f pycharm-docker-mac-debug/docker/Dockerfile \
     --build-arg AUTH_KEY="$(cat pycharm-docker-mac-debug/ssh_keys/docker.pub)" \
     -t mrq-debug .
     
# -v $(pwd):$(pwd) will align our pycharm project's directory structure with docker's
# $HOME/.pycharm_helpers:/root/.pycharm_helpers will get us the helpers we copied earlier
docker run -d -p 127.0.0.1:5022:5022 -v $(pwd):$(pwd) -v $HOME/.pycharm_helpers:/root/.pycharm_helpers mrq-debug /usr/sbin/sshd -D # run sshd without detaching