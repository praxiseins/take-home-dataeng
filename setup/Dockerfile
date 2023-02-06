FROM python:3-alpine

RUN mkdir /app
WORKDIR /app

# Copies
COPY requirements.txt .
COPY python_runner.sh .

RUN \
 apk add bash &&\
 apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
 python3 -m pip install -r requirements.txt --no-cache-dir && \
 apk --purge del .build-deps

# Specify the src volume
VOLUME [ "/app/src" ]
VOLUME [ "/app/pipes" ]

# Run the pipepline
RUN  chmod +x ./python_runner.sh
ENTRYPOINT ["./python_runner.sh"]