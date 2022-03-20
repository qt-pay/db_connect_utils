
# 数据库连接驱动配置信息
DATABASE_INFO = {
    'mysql': {
        'jars': 'mysql-connector-java-8.0.17.jar',
        'drive': 'com.mysql.cj.jdbc.Driver',
    },
    'tdmysql': {
        'jars': 'mysql-connector-java-8.0.17.jar',
        'drive': 'com.mysql.cj.jdbc.Driver',
    },
    'postgres': {
        'jars': 'postgresql-42.2.5.jar',
        'drive': 'org.postgresql.Driver',
    },
    'tdpg': {
        'jars': 'postgresql-42.2.5.jar',
        'drive': 'org.postgresql.Driver',
    },
    'oracle': {
        'jars': 'ojdbc8.jar',
        'drive': 'oracle.jdbc.driver.OracleDriver',
    },
    'vertica': {
        'jars': 'vertica-jdbc-8.1.1-5.jar',
        'drive': 'com.vertica.jdbc.Driver',
    },
    'gauss': {
        'jars': 'gsjdbc4.jar',
        'drive': 'org.postgresql.Driver',
    },
    'hive': {
        'jars': 'hive-jdbc-uber-2.6.5.0-292.jar',
        'drive': 'org.apache.hive.jdbc.HiveDriver',
    },
    'odps': {
        'jars': 'hive-jdbc-uber-2.6.5.0-292.jar',
        'drive': 'org.apache.hive.jdbc.HiveDriver',
    },
}
