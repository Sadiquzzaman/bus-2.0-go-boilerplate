-- Runs once when the postgres volume is created.
-- One database per service. New services: `make db svc=<name>`.
CREATE DATABASE example_pg;
