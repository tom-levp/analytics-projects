BEGIN;

ALTER TABLE products
ADD COLUMN manufacturer VARCHAR(30);

UPDATE products
SET
    manufacturer =
        CASE
            WHEN category = 'CPU' THEN
                CASE
                    WHEN title LIKE '%Intel%' THEN 'INTEL'
                    WHEN title LIKE '%AMD%' THEN 'AMD'
                    ELSE NULL
                END
            WHEN category = 'GPU' THEN
                CASE
                    WHEN model = '' AND title LIKE '%GeForce%' THEN 'NVIDIA'
                    WHEN model = '' AND title LIKE '%Radeon%' THEN 'AMD'
                    WHEN model != '' AND split_part(model, ' ', 1) = '2x' THEN 'AMD'
                    WHEN model LIKE '%NVIDIA%' THEN 'NVIDIA'
                    WHEN model LIKE '%AMD%' THEN 'AMD'
                END
            ELSE ''
        END,
    model =
        CASE
            WHEN category = 'CPU' THEN ''
            ELSE model
        END;

DELETE FROM products
WHERE title LIKE '%+%'
OR model LIKE '%2x%'
OR (
    category = 'GPU'
    AND model = ''
)
OR (
    category = 'GPU'
    AND manufacturer IS NULL
);

-- Create a new table with some of the columns
CREATE TABLE gpu_prices AS
SELECT *
FROM products
WHERE category = 'GPU';

-- Create a new table with the remaining columns
CREATE TABLE cpu_prices AS
SELECT *
FROM products
WHERE category = 'CPU';

UPDATE cpu_prices
SET model = substring(title from '\((.*?)\)'),
    title = regexp_replace(title, '\s\(.*?\)', '');

UPDATE cpu_prices
SET model = substring(description from '(\d+\.\d+ GHz?)')
WHERE model IS NULL;

DELETE FROM cpu_prices
WHERE model IS NULL;

ALTER TABLE cpu_prices
ADD COLUMN core_count VARCHAR(30);

UPDATE cpu_prices
SET core_count = substring(description from '(\d+|\w+?)(-|\s)Core');

DELETE FROM cpu_prices
WHERE core_count IS NULL;

UPDATE cpu_prices
SET
    core_count =
        CASE
            WHEN core_count = 'Single' THEN '1'
            WHEN core_count = 'Dual' THEN '2'
            WHEN core_count = 'Triple' THEN '3'
            WHEN core_count = 'Quad' THEN '4'
            WHEN core_count = 'Hexa' THEN '6'
            ELSE core_count
        END;

ALTER TABLE cpu_prices RENAME COLUMN model TO clock_speed_ghz;

UPDATE cpu_prices
SET clock_speed_ghz = regexp_replace(clock_speed_ghz, '(\sGHz?)', '', 'g');

UPDATE cpu_prices
SET clock_speed_ghz = REPLACE(clock_speed_ghz, 'Max.', 'Max Turbo');

ALTER TABLE cpu_prices
ADD COLUMN process_µm NUMERIC;

UPDATE cpu_prices
SET 
    process_µm =
        CASE
            WHEN title IN (
                'AMD Ryzen 3 4300G Wraith Stealth',
                'AMD Ryzen 3 PRO 4350G',
                'AMD Ryzen 5 4600G Wraith Stealth',
                'AMD Ryzen 5 PRO 4650G'
            )
            THEN 0.007
            ELSE
                CASE
                    WHEN CAST(substring(description from '(\d+\.\d+?)\smicron') AS NUMERIC) IS NULL
                    THEN
                        CASE
                            WHEN CAST(substring(description from '(\d+?)\snm') AS NUMERIC) / 1000 = 0
                            THEN NULL
                            ELSE ROUND(CAST(substring(description from '(\d+?)\snm') AS NUMERIC) / 1000, 3)
                        END
                    ELSE CAST(substring(description from '(\d+\.\d+?)\smicron') AS NUMERIC)
                END
        END;

ALTER TABLE gpu_prices
ADD COLUMN vram_gb NUMERIC;

UPDATE gpu_prices
SET 
    vram_gb =
        CASE
            WHEN
                description LIKE '%Go%'
                OR description LIKE '%GB%'
            THEN CAST(substring(description from '(\d+?)[\s-]*(GB|Go)') AS NUMERIC)
            WHEN
                description LIKE '%Mo%'
                OR description LIKE '%MB%'
            THEN ROUND(CAST(substring(description from '(\d+?)[\s-]*(Mo|MB)') AS NUMERIC) / 1024, 1)
            WHEN
                description NOT LIKE '%Go%'
                AND description NOT LIKE '%GB%'
                AND description NOT LIKE '%Mo%'
                AND description NOT LIKE '%MB%'
            THEN CAST(substring(title from '(\d+?)[\s-](Go|GB)') AS NUMERIC)
        END;

UPDATE gpu_prices
SET model = 
	CASE
		WHEN model LIKE '%avec CUDA%'
		THEN REPLACE(model, 'avec CUDA ', '')
		ELSE CASE
			WHEN model LIKE '%Gb/s%'
			THEN REPLACE(model, 'Gb/s', 'GB')
			ELSE model
		END
	END;

DELETE FROM gpu_prices
WHERE vram_gb IS NULL;

DELETE FROM gpu_prices WHERE price IS NULL;

DELETE FROM gpu_prices
WHERE model LIKE '%x2%';

UPDATE gpu_prices
SET model = REGEXP_REPLACE(model, 'NVIDIA\s*|AMD\s*', '', 'g');

UPDATE cpu_prices
SET clock_speed_ghz = TRIM(REGEXP_REPLACE(clock_speed_ghz, '[^0-9./\s]', '', 'g'))
WHERE clock_speed_ghz LIKE '%Max%' OR clock_speed_ghz LIKE '%Ghz%';

UPDATE cpu_prices
SET clock_speed_ghz = REPLACE(clock_speed_ghz, '/', ' /')
WHERE clock_speed_ghz ~ '/'
AND clock_speed_ghz !~ ' \/';

UPDATE cpu_prices
SET clock_speed_ghz =
  CASE 
    WHEN clock_speed_ghz LIKE '% / %'
	THEN ROUND(((SPLIT_PART(clock_speed_ghz, ' / ', 1)::numeric + SPLIT_PART(clock_speed_ghz, ' / ', 2)::numeric) / 2)::numeric, 1) 
    ELSE clock_speed_ghz::numeric 
  END;

ALTER TABLE cpu_prices
ALTER COLUMN clock_speed_ghz TYPE numeric
USING clock_speed_ghz::numeric,
ALTER COLUMN core_count TYPE numeric
USING core_count::numeric;

DELETE FROM cpu_prices WHERE price IS NULL;

UPDATE cpu_prices
SET title = REPLACE(title, ' Low Noise', '')
WHERE title LIKE '%Low Noise%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Wraith', '')
WHERE title LIKE '%Wraith%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Spire', '')
WHERE title LIKE '%Spire%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Gold', '')
WHERE title LIKE '%Gold%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Black', '')
WHERE title LIKE '%Black%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Edition', '')
WHERE title LIKE '%Edition%';
UPDATE cpu_prices
SET title = REPLACE(title, ' avec mise à jour BIOS', '')
WHERE title LIKE '%avec mise à jour BIOS%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Stealth', '')
WHERE title LIKE '%Stealth%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Stepping B0', '')
WHERE title LIKE '%Stepping B0%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Cooler', '')
WHERE title LIKE '%Cooler%';
UPDATE cpu_prices
SET title = REPLACE(title, ' LED', '')
WHERE title LIKE '%LED%';
UPDATE cpu_prices
SET title = REPLACE(title, ' RGB', '')
WHERE title LIKE '%RGB%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Prism', '')
WHERE title LIKE '%Prism%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Extreme', '')
WHERE title LIKE '%Extreme%';
UPDATE cpu_prices
SET title = REPLACE(title, ' Limited Edition 40th Anniversary', '')
WHERE title LIKE '%Limited Edition 40th Anniversary%';
UPDATE cpu_prices
SET title = REPLACE(title, ' -', '')
WHERE title LIKE '% -%';

ALTER TABLE cpu_prices RENAME COLUMN title TO model;

UPDATE cpu_prices
SET model = SUBSTRING(model FROM POSITION(' ' IN model)+1);

REINDEX TABLE gpu_prices;
REINDEX TABLE cpu_prices;