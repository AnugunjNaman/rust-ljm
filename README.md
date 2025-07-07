# Project 4713

Setup

- docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:latest -js
- telnet 127.0.0.1 4222
- nats context add local --server nats://127.0.0.1:4222
- nats context select local


