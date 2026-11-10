-- Prompt tags for the initial M1 catalog. Item IDs match game.catalog_items.

INSERT INTO imaging.render_catalog (item_id, kind, positive_tags, negative_tags) VALUES
    ('casual-denim',      'outfit',     '{denim_jacket,white_shirt,jeans,sneakers}', '{}'),
    ('summer-dress',      'outfit',     '{sundress,white_dress,sandals,straw_hat}',  '{}'),
    ('batik-modern',      'outfit',     '{batik,long_sleeves,pencil_skirt}',         '{}'),
    ('streetwear-hoodie', 'outfit',     '{hoodie,cargo_pants,baseball_cap}',         '{}'),
    ('standing-wave',     'pose',       '{standing,waving,full_body}',               '{}'),
    ('sitting-cafe',      'pose',       '{sitting,chair,holding_cup}',               '{}'),
    ('walking-street',    'pose',       '{walking,looking_at_viewer,full_body}',     '{}'),
    ('smile',             'expression', '{smile,open_mouth}',                        '{}'),
    ('wink',              'expression', '{one_eye_closed,smile}',                    '{}'),
    ('calm',              'expression', '{closed_mouth,light_smile}',                '{}');
