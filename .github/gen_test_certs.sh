#!/bin/bash

cd certs/

echo "[+] Generating CA"
openssl genrsa -out ca.key.pem 2048
openssl req -x509 -new -nodes -key ca.key.pem -subj "/CN=CARoot" -days 365 -out ca.cert.pem

echo "[+] Generating server key"
openssl genrsa -out server.key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -in server.key.pem -out server.key-pk8.pem -nocrypt

echo "[+] Generating server cert"
openssl req -new -config server.conf -key server.key.pem -out server.csr.pem -sha256
openssl x509 -req -in server.csr.pem -CA ca.cert.pem -CAkey ca.key.pem -CAcreateserial -out server.cert.pem -days 365 -extensions v3_ext -extfile server.conf -sha256

echo "[+] Generating client key"
openssl genrsa -out client.key.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -in client.key.pem -out client.key-pk8.pem -nocrypt

echo "[+] Generating client cert"
openssl req -new -subj "/CN=client" -key client.key.pem -out client.csr.pem -sha256
openssl x509 -req -in client.csr.pem -CA ca.cert.pem -CAkey ca.key.pem -CAcreateserial -out client.cert.pem -days 365 -sha256

chmod 555 *