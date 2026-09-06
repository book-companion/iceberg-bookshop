-- One PostgreSQL 18 for the lab, three databases: the two production catalogs'
-- metadata stores (stage 2) and, later, the bookshop operational source for
-- Debezium (stage 4, seeded from ../sql-verify/).
CREATE DATABASE polaris;
CREATE DATABASE lakekeeper;
CREATE DATABASE bookshop;
