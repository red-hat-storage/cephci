FROM alpine

RUN apk add --no-cache fio==3.36-r0

WORKDIR /

COPY . .

ENTRYPOINT [ "fio" ]
