import happybase

def test_hbase_connection():
    try:
        connection = happybase.Connection('hbase-master', port=9090)
        connection.open()
        print("Successfully connected to HBase!")
        connection.close()
    except Exception as e:
        print(f"Error connecting to HBase: {e}")

test_hbase_connection()
