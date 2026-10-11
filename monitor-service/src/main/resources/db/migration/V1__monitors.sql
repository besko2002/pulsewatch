CREATE TABLE organizations (
 id uuid PRIMARY KEY, name text NOT NULL, slug text NOT NULL UNIQUE
);
CREATE TABLE monitors (
 id uuid PRIMARY KEY, org_id uuid NOT NULL REFERENCES organizations(id), name text NOT NULL,
 url text NOT NULL, expected_status integer NOT NULL, interval_seconds integer NOT NULL CHECK (interval_seconds >= 30),
 version bigint NOT NULL DEFAULT 0
);
CREATE INDEX monitors_org_id_idx ON monitors(org_id);
