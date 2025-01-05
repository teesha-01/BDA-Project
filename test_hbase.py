import happybase

connection = happybase.Connection('localhost', port=9091)
print(connection.tables())
