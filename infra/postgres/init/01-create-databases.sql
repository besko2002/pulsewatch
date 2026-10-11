CREATE USER monitor_user WITH PASSWORD 'monitor_pass';
CREATE USER telemetry_user WITH PASSWORD 'telemetry_pass';
CREATE USER incident_user WITH PASSWORD 'incident_pass';
CREATE USER notification_user WITH PASSWORD 'notification_pass';
CREATE USER keycloak_user WITH PASSWORD 'keycloak_pass';
CREATE DATABASE monitor_db OWNER monitor_user;
CREATE DATABASE telemetry_db OWNER telemetry_user;
CREATE DATABASE incident_db OWNER incident_user;
CREATE DATABASE notification_db OWNER notification_user;
CREATE DATABASE keycloak_db OWNER keycloak_user;
\connect telemetry_db
CREATE EXTENSION IF NOT EXISTS timescaledb;
GRANT USAGE ON SCHEMA public TO telemetry_user;
