CREATE OR REPLACE VIEW content_dim_view AS 
SELECT 
    id::text, 
    slug, 
    title, 
    content_type, 
    length_seconds, 
    publish_ts 
FROM content;
