FROM public.ecr.aws/lambda/python:3.11

RUN yum update
RUN yum install -y git
RUN yum install -y make
RUN yum install -y gcc
RUN yum install -y gcc-c++

RUN git clone https://github.com/official-stockfish/Stockfish.git /tmp/stockfish
RUN cd /tmp/stockfish/src && make -j profile-build ARCH=x86-64
RUN cd /tmp/stockfish/src && make install

COPY . ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]
