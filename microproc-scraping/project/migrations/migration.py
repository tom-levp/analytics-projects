import psycopg2
from psycopg2 import sql


def migrate():
    """ Runs the queries listed in the 'queries.sql' file. """

    conn = psycopg2.connect(database="electronics", user="postgres", password="your_password", host="localhost", port=5432)
    cursor = conn.cursor()

    cursor.execute(f"""
    
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
        CREATE TABLE gpu AS
        SELECT *
        FROM products
        WHERE category = 'GPU';

        -- Create a new table with the remaining columns
        CREATE TABLE cpu AS
        SELECT *
        FROM products
        WHERE category = 'CPU';

        UPDATE cpu
        SET model = substring(title from '\((.*?)\)'),
            title = regexp_replace(title, '\s\(.*?\)', '');

        UPDATE cpu
        SET model = substring(description from '(\d+\.\d+ GHz?)')
        WHERE model IS NULL;

        DELETE FROM cpu
        WHERE model IS NULL;

        ALTER TABLE cpu
        ADD COLUMN core_count VARCHAR(30);

        UPDATE cpu
        SET core_count = substring(description from '(\d+|\w+?)(-|\s)Core');

        DELETE FROM cpu
        WHERE core_count IS NULL;

        UPDATE cpu
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

        ALTER TABLE cpu RENAME COLUMN model TO clock_speed_ghz;

        UPDATE cpu
        SET clock_speed_ghz = regexp_replace(clock_speed_ghz, '(\sGHz?)', '', 'g');

        UPDATE cpu
        SET clock_speed_ghz = REPLACE(clock_speed_ghz, 'Max.', 'Max Turbo');

        ALTER TABLE cpu
        ADD COLUMN process_µm NUMERIC;

        UPDATE cpu
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

        ALTER TABLE gpu
        ADD COLUMN vram_gb NUMERIC;

        UPDATE gpu
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

        DELETE FROM gpu
        WHERE vram_gb IS NULL;

        UPDATE gpu
        SET model = REGEXP_REPLACE(model, 'NVIDIA\s*|AMD\s*', '', 'g');

        REINDEX TABLE gpu;
        REINDEX TABLE cpu;

        """)

    conn.commit()
    cursor.close()
    conn.close()


def rollback():
    """ Drops 'cpu_prices' and 'gpu_prices' tables. """

    conn = psycopg2.connect(database="electronics", user="postgres", password="your_password", host="localhost", port=5432)
    cursor = conn.cursor()

    # cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier('my_table')))

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    migrate()
    # rollback()