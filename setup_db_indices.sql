-- Time scan: BRIN is essentially free for append-only event logs
CREATE INDEX ON stats1.record_view_events USING brin ("timestamp");

-- Per-community time slices (the most common dashboard query)
CREATE INDEX ON stats1.record_view_events USING gin (community_ids);

-- contains-any lookup
CREATE INDEX ON stats1.record_view_events ("timestamp", access_status);

-- bucket + facet
CREATE INDEX ON stats1.record_view_events ("timestamp", resource_type_id);

CREATE INDEX ON stats1.record_view_events (record_id, "timestamp");

-- per-record traffic
-- Snapshot facets that live inside JSONB
CREATE INDEX ON stats1.record_view_events USING gin (record_metadata_snapshot jsonb_path_ops);

-- If a specific nested .id is queried *a lot*, give it an expression index:
CREATE INDEX ON stats1.record_view_events USING gin ((record_metadata_snapshot -> 'languages'));

CREATE INDEX ON stats1.record_view_events USING gin ((record_metadata_snapshot -> 'subjects'));

CREATE INDEX ON stats1.record_view_events USING gin ((record_metadata_snapshot -> 'affiliations'));

CREATE INDEX ON stats1.record_view_events USING gin ((record_metadata_snapshot -> 'funders'));
