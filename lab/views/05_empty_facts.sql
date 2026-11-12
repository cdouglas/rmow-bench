-- Facts that only some runs produce, as empty tables when no run has them, so the views that read
-- them still build. rg_meta is the row groups of the version dumps (--dump-versions); no run that
-- stands since 2026-11-12 has one. The columns are those the views read.
CREATE TABLE IF NOT EXISTS rg_meta (
    run_id VARCHAR, arm VARCHAR, file_name VARCHAR, row_group_id BIGINT,
    total_compressed_size BIGINT);
