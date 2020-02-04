FROM semtech/mu-jruby-template

LABEL maintainer="Nathaniel Rudavsky-Brody <nathaniel.rudavsky@gmail.com>"
# 200MB
ENV MAXIMUM_FILE_SIZE 209715200
# seconds
ENV ELASTIC_READ_TIMEOUT 180 