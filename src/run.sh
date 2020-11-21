#!/bin/bash
docker-compose -f docker-compose-dev.yaml up --build --scale raw_data_receiver=4 
# docker-compose -f docker-compose-dev.yaml up --build
