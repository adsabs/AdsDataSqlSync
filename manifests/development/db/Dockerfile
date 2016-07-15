
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


#=========== POSTGRESQL ===============

RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python-software-properties software-properties-common postgresql-9.5 postgresql-client-9.5 postgresql-contrib-9.5

#RUN mkdir -p /vagrant/data/postgres
#RUN chown -R postgres:postgres /vagrant/data/postgres

USER postgres

#RUN mkdir -p /var/lib/postgresql/9.5/
#RUN mv /var/lib/postgresql/9.5/main /var/lib/postgresql/9.5/main.old
#RUN ln -s /vagrant/data/postgres/ /var/lib/postgresql/9.5/main
#RUN mv /var/lib/postgresql/9.5/main.old/* /var/lib/postgresql/9.5/main/
#RUN chmod 700 /var/lib/postgresql/9.5/main
#RUN ls -la /var/lib/postgresql/9.5/main

# Create a PostgreSQL role named ``docker`` with ``docker`` as the password and
# then create a database `docker` owned by the ``docker`` role.
# Note: here we use ``&&\`` to run commands one after the other - the ``\``
#       allows the RUN command to span multiple lines.
RUN    /etc/init.d/postgresql start &&\
    psql --command "CREATE USER docker WITH SUPERUSER PASSWORD 'docker';" &&\
    createdb -O docker docker

# Adjust PostgreSQL configuration so that remote connections to the
# database are possible. 
RUN echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/9.5/main/pg_hba.conf

# And add ``listen_addresses`` to ``/etc/postgresql/9.5/main/postgresql.conf``
RUN echo "listen_addresses='*'" >> /etc/postgresql/9.5/main/postgresql.conf

# Expose the PostgreSQL port
EXPOSE 5432

# Add VOLUMEs to allow backup of config, logs and databases
#RUN mkdir -p /var/lib/postgresql/data
#VOLUME  ["/var/lib/postgresql/data"]

USER root
RUN mkdir -p /etc/service/postgres
RUN echo "#!/bin/bash\nexec /sbin/setuser postgres /usr/lib/postgresql/9.5/bin/postgres -c config_file=/etc/postgresql/9.5/main/postgresql.conf" > /etc/service/postgres/run
RUN chmod +x /etc/service/postgres/run