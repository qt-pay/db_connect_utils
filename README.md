# db_connect_utils
- 基于Python3的各类数据库连接和连接池, 支持数据库有: Mysql(MariaDB), Oracle, PostgreSQL(GreenPlum), Vertica, Redis, MongoDB;
- 数据库普通连接和连接池:
        
    | 数据库名称 | 普通连接 | 数据库连接池 |
    | :--------: | :------: | :----------: |
    |   Mysql    |    Y     |      Y       |
    |   Postgresql    |    Y     |      Y       |
    |   Oracle    |    Y     |      Y       |
    |   Vertica    |    Y     |      Y       |
    |   Redis    |    Y     |      Y       |
    |   MongoDB    |    Y     |      N       |
    |   Hive    |    Y     |      Y       |
    |   Impala    |    Y     |      Y       |
    
    `Y 已实现,N未实现` 
    
    
    
## 注意事项

1. 各个数据库连接仅通过了简单测试;
2. 各个数据库连接关键字参数有细微差别, 详见各封装类__init__方法;
3. pyimpala 数据库连接返回字典数据是字节;
4. 帮忙点个⭐️吧, 非常感谢!
