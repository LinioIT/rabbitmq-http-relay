FROM scratch
ADD bin/rabbitmq-worker /bin/
CMD ["/bin/rabbitmq-worker", "/etc/rabbitmq-worker.conf"]
