import psycopg2, pathlib, datetime, csv, json, decimal
# Weirdly enough, I need to make this separate import to get the psycopg2 sql module
from psycopg2 import sql

ROOT_DIR = pathlib.Path(__file__).resolve().parent
DATA_DIR = pathlib.Path(ROOT_DIR.parent, 'data')
DATA_DIR.mkdir(parents=True, exist_ok=True)

HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'your_password'
PORT = 5432
DATABASE_NAME = 'electronics'

TABLE_NAME = 'products'


def db_init():
    """ Checks if database, tables and their constraints exist. 
    If they do not, creates them. """

    # Connect to the default PostgreSQL database
    pg_conn = psycopg2.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        port=PORT,
    )   

    # Check if the database exists
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{DATABASE_NAME}';")
        exists = cur.fetchone()

    # Create the database if it doesn't exist
    if not exists:
        with pg_conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {DATABASE_NAME};")
            pg_conn.commit()

        print(f"Database '{DATABASE_NAME}' successfully created.")
    else:
        print(f"Database '{DATABASE_NAME}' already exists.")

    pg_conn.close()

    db_conn = psycopg2.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        port=PORT,
        database=DATABASE_NAME,
    )

    # Create the 'scraped_urls' table if it doesn't exist
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'scraped_urls'
            );
        """)
        exists = cur.fetchone()[0]

    if not exists:
        try:
            with db_conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE scraped_urls (
                        id SERIAL PRIMARY KEY,
                        url VARCHAR(160),
                        category VARCHAR(30),
                        date DATE NOT NULL,
                        status VARCHAR(7) NOT NULL CHECK (status IN ('PENDING', 'DONE'))
                    );
                """)
                # Add a unique constraint on the url
                cur.execute(f"""
                    CREATE UNIQUE INDEX scraped_urls_url_idx
                    ON scraped_urls (url);
                """)
                db_conn.commit()
            print(f"Table 'scraped_urls' and its constraints successfully created.")
        except Exception as e:
            print("Error creating table 'scraped_urls': {e}")
            db_conn.rollback()
    else:
        print(f"Table 'scraped_urls' and its related constraints already exist.")

    # Create the 'products' table if it doesn't exist
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'products'
            );
        """)
        exists = cur.fetchone()[0]

    if not exists:
        try:
            with db_conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE products (
                        id SERIAL PRIMARY KEY,
                        sku VARCHAR(14),
                        category VARCHAR(30),
                        title VARCHAR(120),
                        description VARCHAR(140),
                        model VARCHAR(120),
                        price NUMERIC,
                        date DATE NOT NULL
                    );
                """)

                # Add a unique constraint on the combination of timestamp and sku
                cur.execute(f"""
                    CREATE UNIQUE INDEX products_date_sku_idx
                    ON products (date, sku);
                """)
                db_conn.commit()
            print(f"Table 'products' successfully created.")
        except Exception as e:
            print(f"Error creating table 'products': {e}")
            db_conn.rollback()
    else:
        print(f"Table 'products' and its related constraints already exist.")

    # Close the connection
    db_conn.close()


def is_prod_proc(conn, date, sku):
    """ Checks whether a product has already been processed. """

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM products
                WHERE date = %s
                AND sku = %s
            );
        """,
        (date, sku))
        exists = cur.fetchone()[0]
    
    if exists:
        return True


def add_data_to_products_tb(conn, dict):
    """ Adds data to product table. """

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO products (sku, category, title, description, model, price, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (dict['sku'], dict['category'], dict['title'], dict['description'], dict['model'], dict['price'], dict['date']))
    except Exception as e:
        print(e)
        for element in dict: print(dict[element])


def init_entry_url_tb(conn, dict):
    """ Checks whether url entry exists in scraped_urls table.
    If it does not, creates the entry. """

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM scraped_urls
                WHERE category = %s
                AND date = %s
            );
        """,
        (dict['category'], dict['date']))
        exists = cur.fetchone()[0]
        
    if exists:
        pass
    else:
        # Add data to the table
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO scraped_urls (url, category, date, status)
                VALUES (%s, %s, %s, %s);
            """,
            (dict['url'], dict['category'], dict['date'], 'PENDING'))
        

def is_url_proc(conn, dict):
    """ Checks whether a url has a 'DONE' status. """

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM scraped_urls
                WHERE url = %s
                AND status = %s
            );
        """,
        (dict['url'], 'DONE'))
        exists = cur.fetchone()[0]
    
    if exists:
        return True


def get_proc_url():
    """ Gets all processed urls as a set of tuples. """

    db_conn = psycopg2.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        port=PORT,
        database=DATABASE_NAME,
    )

    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT category, date
            FROM scraped_urls
            WHERE status = 'DONE';
        """)
        proc_url = set(cur.fetchall())
    
    db_conn.close()

    return proc_url


def update_url_row(conn, dict):
    """ Sets a url entry status to 'DONE'. """

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM scraped_urls
                WHERE url = %s
            );
        """,
        (dict['url'],))
        exists = cur.fetchone()[0]
        
    if exists:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE scraped_urls
                SET status = 'DONE'
                WHERE url = %s;
            """,
            (dict['url'],))
    else:
        # Add data to the table
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO scraped_urls (url, category, date, status)
                VALUES (%s, %s, %s, %s);
            """,
            (dict['url'], dict['category'], dict['date'], 'DONE'))


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)


def export_table(table):
    """ Exports PostgreSQL data to JSON and CSV formats. """

    db_conn = psycopg2.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        port=PORT,
        database=DATABASE_NAME,
    )

    # Create the 'products' table if it doesn't exist
    with db_conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = {}
                )
            """)
            .format(sql.Literal(table))
        )
        exists = cur.fetchone()[0]
    
    if exists:
        with db_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT * FROM {}")
                .format(sql.Identifier(table))
            )
            rows = cur.fetchall()
        
        # Export to CSV
        with open(pathlib.Path(DATA_DIR, 'pg_exports', f'{table}_dataset.csv'), 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([desc[0] for desc in cur.description])
            csvwriter.writerows(rows)

        # Export to JSON
        with open(pathlib.Path(DATA_DIR, 'pg_exports', f'{table}_dataset.json'), 'w', encoding='utf-8') as jsonfile:
            column_names = [desc[0] for desc in cur.description]
            data = [dict(zip(column_names, row)) for row in rows]
            json.dump(data, jsonfile, cls=CustomEncoder, ensure_ascii=False)

    else:
        print(f"Table '{table}' does not exist, it can't be exported.")

    db_conn.close()


def get_table_as_records(table):
    db_conn = psycopg2.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        port=PORT,
        database=DATABASE_NAME,
    )

    with db_conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = {}
                )
            """)
            .format(sql.Literal(table))
        )
        exists = cur.fetchone()[0]
    
    if exists:
        with db_conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT DISTINCT model FROM {}")
                .format(sql.Identifier(table))
            )
            rows = cur.fetchall()

        column_names = [desc[0] for desc in cur.description]
        data = [dict(zip(column_names, row)) for row in rows]

    else:
        print(f"Table '{table}' does not exist, it can't be exported.")

    db_conn.close()

    return data        


def drop_table(conn, table):
    """ Drops a table (products, scraped_urls). """

    # Check if the table already exists
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table = {}
                )
            """)
            .format(sql.Literal(table))
        )
        exists = cur.fetchone()[0]

    if exists:
        # Create a cursor object
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}")
                .format(sql.Identifier(table))
            )
            conn.commit()
    else:
        print(f"Table '{table}' does not exist, it can't be dropped.")

    conn.close()


########################################################################


def create_specs_tables():
    """ Creates specs tables in Postgres. """

    db_conn = psycopg2.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        port=PORT,
        database=DATABASE_NAME,
    )

    # Create the 'cpu_specs' table if it doesn't exist
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'cpu_specs'
            );
        """)
        exists = cur.fetchone()[0]

    if not exists:
        try:
            with db_conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE cpu_specs (
                        id SERIAL PRIMARY KEY,
                        model VARCHAR(40),
                        process_size_nm NUMERIC,
                        transistor_count NUMERIC,
                        die_size_mm2 NUMERIC,
                        launch_price_usd NUMERIC,
                        release_date DATE,
                        core_count NUMERIC,
                        thread_count NUMERIC,
                        frequency_ghz NUMERIC,
                        tdp_w NUMERIC,
                        foundry VARCHAR(20)
                    );
                """)
                # Add a unique constraint on the model column
                cur.execute(f"""
                    CREATE UNIQUE INDEX cpu_specs_model_idx
                    ON cpu_specs (model);
                """)
                db_conn.commit()
            print(f"Table 'cpu_specs' and its constraints successfully created.")
        except Exception as e:
            print("Error creating table 'cpu_specs': {e}")
            db_conn.rollback()
    else:
        print(f"Table 'cpu_specs' and its related constraints already exist.")


    # Create the 'gpu_specs' table if it doesn't exist
    with db_conn.cursor() as cur:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'gpu_specs'
            );
        """)
        exists = cur.fetchone()[0]

    if not exists:
        try:
            with db_conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE gpu_specs (
                        id SERIAL PRIMARY KEY,
                        model VARCHAR(30),
                        architecture VARCHAR(20),
                        process_size_nm NUMERIC,
                        transistor_count NUMERIC,
                        density_m_per_mm2 VARCHAR(120),
                        die_size_mm2 NUMERIC,
                        tdp_w NUMERIC,
                        memory_size_gb NUMERIC,
                        memory_type VARCHAR(12),
                        launch_price_usd NUMERIC,
                        release_date DATE,
                        tensor_core_count NUMERIC,
                        pixel_rate_gpixel_per_s NUMERIC,
                        texture_rate_gtexel_per_s NUMERIC,
                        fp32_tflops NUMERIC,
                        base_clock_mhz NUMERIC,
                        boost_clock_mhz NUMERIC,
                        foundry VARCHAR(20)
                    );
                """)
                # Add a unique constraint on the model column
                cur.execute(f"""
                    CREATE UNIQUE INDEX gpu_specs_model_idx
                    ON gpu_specs (model);
                """)
                db_conn.commit()
            print(f"Table 'gpu_specs' successfully created.")
        except Exception as e:
            print(f"Error creating table 'gpu_specs': {e}")
            db_conn.rollback()
    else:
        print(f"Table 'gpu_specs' and its related constraints already exist.")

    # Close the connection
    db_conn.close()


def add_data_to_cpu_specs_tb(conn, dict):
    """ Adds data to cpu_specs table. """

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO cpu_specs (model, process_size_nm, transistor_count, die_size_mm2, launch_price_usd, release_date, core_count, thread_count, frequency_ghz, tdp_w, foundry)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (dict['model'], dict['process_size_nm'], dict['transistor_count'], dict['die_size_mm2'], dict['launch_price_usd'], dict['release_date'], dict['core_count'], dict['thread_count'], dict['frequency_ghz'], dict['tdp_w'], dict['foundry']))
    except Exception as e:
        print(e)
        conn.rollback()
        for element in dict: print(dict[element])


def add_data_to_gpu_specs_tb(conn, dict):
    """ Adds data to gpu_specs table. """

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO gpu_specs (model, architecture, process_size_nm, transistor_count, density_m_per_mm2, die_size_mm2, tdp_w, memory_size_gb, memory_type, launch_price_usd, release_date, tensor_core_count, pixel_rate_gpixel_per_s, texture_rate_gtexel_per_s, fp32_tflops, base_clock_mhz, boost_clock_mhz, foundry)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (dict['model'], dict['architecture'], dict['process_size_nm'], dict['transistor_count'], dict['density_m_per_mm2'], dict['die_size_mm2'], dict['tdp_w'], dict['memory_size_gb'], dict['memory_type'], dict['launch_price_usd'], dict['release_date'], dict['tensor_core_count'], dict['pixel_rate_gpixel_per_s'], dict['texture_rate_gtexel_per_s'], dict['fp32_tflops'], dict['base_clock_mhz'], dict['boost_clock_mhz'], dict['foundry']))
    except Exception as e:
        print(e)
        conn.rollback()
        for element in dict: print(dict[element])


if __name__ == '__main__':
    export_table('gpu_prices')
    export_table('cpu_prices')
    export_table('gpu_specs')
    export_table('cpu_specs')
    # print('Hello World')