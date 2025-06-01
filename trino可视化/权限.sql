hiveServer2 中有权限如下

CREATE ROLE trino;
CREATE GROUP trino_group;
GRANT ALL PRIVILEGES ON DATABASE huntress_om_test TO ROLE trino;
GRANT ROLE trino TO GROUP trino_group;
ALTER GROUP trino_group ADD USER xiduo;
ALTER GROUP trino_group ADD USER taskdeploy;




superset -> trino

使用超级管理员账号链接trino数据源
启动账号模拟功能.  trino操作账号即为 superset登录用户



trino service 用户模拟添加如下权限

{
    "impersonation": [
	    {
	        "original_user": "taskdeploy",
	        "new_user": ".*"
	    }
    ]
}