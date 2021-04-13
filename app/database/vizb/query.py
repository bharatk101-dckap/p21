create_p21_table = """
CREATE TABLE IF NOT EXISTS p21 (
	company_id varchar PRIMARY KEY,
        server_id varchar,
	port_id varchar,
	database_id varchar,
	user_id varchar, 
	password varchar
);
"""



create_table_query = [create_p21_table]
