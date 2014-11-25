def mysql_config():
    """
    MySQL connection configuration elements. Fill out the appropriate data and
    rename the file to config.py
    """
    config = {
      'user': 'your_db_username',
      'password': 'your_db_password',
      'host': 'your_db_hostname',
      'database': 'your_db_schema',
      'raise_on_warnings': True
    }
    return config