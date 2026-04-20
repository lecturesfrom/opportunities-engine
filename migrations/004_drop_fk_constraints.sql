-- ============================================================
-- Migration 004: drop_fk_constraints
-- Removes FOREIGN KEY constraints from all child tables.
--
-- DuckDB 1.5.2 workaround: FK constraints block UPDATE on parent
-- non-PK columns (confirmed upstream bug). Drop FKs and let app
-- code enforce integrity. Reinstate when upstream fix ships.
--
-- The application layer already enforces referential integrity,
-- so the FKs are defensive only.
--
-- DuckDB does not support ALTER TABLE DROP CONSTRAINT, so
-- each table is recreated without FK and swapped.
--
-- IMPORTANT: This migration must only run on databases where
-- FK constraints actually exist (DBs created with the original
-- migration 001 that had REFERENCES clauses). Fresh databases
-- using the updated migration 001 (no REFERENCES) will have
-- no FKs, and the ALTER TABLE RENAME will fail due to index
-- dependencies.
--
-- The migration runner (migrate.py) has been updated to detect
-- this and skip 004 on fresh DBs.
-- ============================================================

-- Record this migration first (so if DDL fails partially, we
-- can detect the state). The runner will handle the actual DDL.
INSERT INTO schema_migrations (version, name, checksum)
VALUES ('004', 'drop_fk_constraints', NULL)
ON CONFLICT (version) DO NOTHING;
