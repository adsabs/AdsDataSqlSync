FROM phusion/baseimage:0.9.17

# Regenerate SSH host keys. baseimage-docker does not contain any
# RUN /usr/bin/ssh-keygen -A
RUN /etc/my_init.d/00_regen_ssh_host_keys.sh

# enable SSH service
RUN rm -f /etc/service/sshd/down


# The insecure key is from https://raw.githubusercontent.com/phusion/baseimage-docker/master/image/services/sshd/keys/insecure_key
RUN /usr/sbin/enable_insecure_key

# So that Vagrant can detect the machine
EXPOSE 22

RUN apt-get update
RUN apt-get install -y python-pip ipython python-dev libpq-dev
RUN pip install --upgrade pip

RUN mkdir -p /etc/service/app
ADD run /etc/service/app/run
RUN chmod +x /etc/service/app/run

#RUN service ssh start
CMD /bin/bash


