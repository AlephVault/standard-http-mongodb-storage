#!/bin/bash
BASEDIR=$(dirname $(dirname "$0"))
PYTHONPATH=$BASEDIR MONGODB_USER=admin MONGODB_PASSWORD=p455w0rd python