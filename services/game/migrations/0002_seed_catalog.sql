-- Seed data for M1. These destinations, background photos, and catalog items are
-- temporary: real Mapillary photos and their anchors are curated in the admin web app in M7.
-- The upserts keep this file safe on a database seeded by an earlier version of it.

INSERT INTO game.destinations
    (destination_id, name, level, parent_id, country_code, region_code,
     latitude, longitude, timezone, follower_threshold, description)
VALUES
    ('id-jakarta-kota-tua', 'Kota Tua Jakarta', 'city', NULL, 'ID', 'ID-JK',
     -6.135200, 106.813300, 'Asia/Jakarta', 0,
     'The old town quarter, with colonial buildings and the Fatahillah Museum.'),
    ('id-bandung-braga', 'Jalan Braga, Bandung', 'city', NULL, 'ID', 'ID-JB',
     -6.917500, 107.609000, 'Asia/Jakarta', 200,
     'A historic street of cafes, galleries, and art deco buildings.'),
    ('id-yogyakarta-malioboro', 'Malioboro, Yogyakarta', 'city', NULL, 'ID', 'ID-YO',
     -7.792800, 110.365800, 'Asia/Jakarta', 1000,
     'The most famous shopping street in Yogyakarta, busy until late at night.'),
    ('id-bali-ubud', 'Ubud, Bali', 'city', NULL, 'ID', 'ID-BA',
     -8.506900, 115.262500, 'Asia/Makassar', 5000,
     'A center of art and terraced rice fields in the middle of Bali.')
ON CONFLICT (destination_id) DO UPDATE
    SET name = EXCLUDED.name, description = EXCLUDED.description;

INSERT INTO game.destination_photos
    (photo_id, destination_id, mapillary_image_id, s3_key, photographer, license,
     anchor_foot_x, anchor_foot_y, anchor_scale, light_azimuth_deg)
VALUES
    ('0193a000-0000-7000-8000-000000000101', 'id-jakarta-kota-tua', 'seed-jakarta-1',
     'assets/destinations/id-jakarta-kota-tua/1.jpg', 'Not curated yet', 'CC BY-SA 4.0', 0.50, 0.88, 1.00, 250),
    ('0193a000-0000-7000-8000-000000000102', 'id-jakarta-kota-tua', 'seed-jakarta-2',
     'assets/destinations/id-jakarta-kota-tua/2.jpg', 'Not curated yet', 'CC BY-SA 4.0', 0.42, 0.90, 0.95, 110),
    ('0193a000-0000-7000-8000-000000000201', 'id-bandung-braga', 'seed-bandung-1',
     'assets/destinations/id-bandung-braga/1.jpg', 'Not curated yet', 'CC BY-SA 4.0', 0.55, 0.86, 1.05, 200),
    ('0193a000-0000-7000-8000-000000000301', 'id-yogyakarta-malioboro', 'seed-yogyakarta-1',
     'assets/destinations/id-yogyakarta-malioboro/1.jpg', 'Not curated yet', 'CC BY-SA 4.0', 0.48, 0.89, 1.00, 270),
    ('0193a000-0000-7000-8000-000000000401', 'id-bali-ubud', 'seed-bali-1',
     'assets/destinations/id-bali-ubud/1.jpg', 'Not curated yet', 'CC BY-SA 4.0', 0.50, 0.87, 0.90, 90)
ON CONFLICT (photo_id) DO UPDATE
    SET photographer = EXCLUDED.photographer;

INSERT INTO game.catalog_items (item_id, kind, name, style_tags) VALUES
    ('casual-denim',      'outfit',     'Casual denim jacket',       '{casual,street}'),
    ('summer-dress',      'outfit',     'Summer dress',              '{feminine,summer}'),
    ('batik-modern',      'outfit',     'Modern batik',              '{traditional,batik}'),
    ('streetwear-hoodie', 'outfit',     'Streetwear hoodie',         '{street,sporty}'),
    ('standing-wave',     'pose',       'Standing and waving',       '{cheerful}'),
    ('sitting-cafe',      'pose',       'Sitting at a cafe',         '{relaxed,cafe}'),
    ('walking-street',    'pose',       'Walking down the sidewalk', '{candid,street}'),
    ('smile',             'expression', 'Smile',                     '{cheerful}'),
    ('wink',              'expression', 'Wink',                      '{playful}'),
    ('calm',              'expression', 'Calm',                      '{elegant}')
ON CONFLICT (item_id) DO UPDATE
    SET name = EXCLUDED.name;
