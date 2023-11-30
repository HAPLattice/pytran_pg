#! /usr/bin/env bash

docker run -d --name test_pg -p 5432:5432 -e "POSTGRES_USER=postgres" -e "POSTGRES_PASSWORD=postgres" -e "POSTGRES_DATABASE=postgres" -v ./pytran_pg.conf:/etc/postgresql/config/ptran_pg.conf postgres:15 postgres -c config_file=/etc/postgresql/config/ptran_pg.conf

