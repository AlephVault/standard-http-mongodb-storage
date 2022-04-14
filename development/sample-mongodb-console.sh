#!/bin/bash
BASEDIR=$(dirname $(dirname "$0"))
MONGODB_USER=admin MONGODB_PASSWORD=p455w0rd python -i $BASEDIR/http_storage/mongo_client.py