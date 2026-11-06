-- The arms and their encoding, from analysis/plots.py (ARMS, HEADLINE): hue is the family, shape
-- the variant within it (a Vega shape name). ord is plots.py's order, used for legends and tables.
CREATE TABLE arm_meta AS
SELECT * FROM (VALUES
    (1, 'mor-v2-file', 'mor', 'circle', 'MoR v2 file', false),
    (2, 'mor-v2-partition', 'mor', 'square', 'MoR v2 partition', false),
    (3, 'mor-v3', 'mor', 'triangle-up', 'MoR v3 DV', true),
    (4, 'cow', 'cow', 'circle', 'CoW', false),
    (5, 'tailcow-b', 'cow', 'square', 'TailCoW-B', false),
    (6, 'tailcow-c', 'cow', 'triangle-up', 'TailCoW-C', true),
    (10, 'rmor-c', 'rmor', 'triangle-up', 'RMoR-C', true),
    (11, 'rmor-c-footer', 'rmor', 'square', 'RMoR-C footer', true),
    (16, 'rmor-c-kept', 'rmor', 'diamond', 'RMoR-C kept', false),
    (17, 'rmor-c-footer-kept', 'rmor', 'cross', 'RMoR-C footer kept', false),
    (18, 'rmor-c-clean', 'rmor', 'triangle-down', 'RMoR-C clean', false),
    (19, 'rmor-density', 'rmor', 'wedge', 'RMoR density', false),
    (7, 'rmow-a', 'rmow', 'circle', 'RMoW-A', false),
    (8, 'rmow-b', 'rmow', 'square', 'RMoW-B', true),
    (9, 'rmow-c', 'rmow', 'triangle-up', 'RMoW-C', true),
    (13, 'rmow-a-split', 'rmowsplit', 'circle', 'RMoW-A-split', false),
    (14, 'rmow-b-split', 'rmowsplit', 'square', 'RMoW-B-split', true),
    (15, 'rmow-c-split', 'rmowsplit', 'triangle-up', 'RMoW-C-split', true)
) t(ord, arm, family, marker, arm_label, headline);
