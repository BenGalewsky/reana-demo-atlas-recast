FROM atlas/analysisbase:latest
ADD . /code
RUN sudo sh -c "source /home/atlas/release_setup.sh && pip install hftools"

USER root

RUN yum update -y && \
    yum groupinstall "Development Tools" -y && \
    wget https://www.python.org/ftp/python/3.6.8/Python-3.6.8.tgz && \
    tar -xvf Python-3.6.8.tgz && \
    cd Python-3.6.8 && \
    ./configure && \
    make && \
    make altinstall && \
    /usr/local/bin/pip3.6 install cython && \
    /usr/local/bin/pip3.6 install parsl

# Clean up after python build
RUN rm -f Python-3.6.8.tgz && \
    rm -Rf Python-3.6.8

