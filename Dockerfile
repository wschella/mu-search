FROM semtech/mu-jruby-template

LABEL maintainer="Nathaniel Rudavsky-Brody <nathaniel.rudavsky@gmail.com>"
ENV NUMBER_OF_THREADS 32
ENV MAXIMUM_FILE_SIZE = 209715200 # 200MB