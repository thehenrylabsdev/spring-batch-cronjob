-- V001 - Create dedicated Quartz schema (SPEC 16/17).
-- All Quartz cluster state lives in its own schema, separated from
-- Spring Batch metadata (batch) and application control tables (control).
CREATE SCHEMA IF NOT EXISTS quartz;
