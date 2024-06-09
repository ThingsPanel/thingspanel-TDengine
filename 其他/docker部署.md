# docker部署TDengine
## 命令
    docker run -tid --name tdengine -v ./taos/dnode/data:/var/lib/taos -v ./taos/dnode/log:/var/log/taos -v ./taos/tmp:/tmp -p 6030:6030 -p 6041-6049:6041-6049 -p 6041-6049:6041-6049/udp tdengine/tdengine

## 默认用户名密码
root taosdata

## TDengine数据库管理工具
DBevaer

# docker部署服务
## 编译服务
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build .
## build镜像
    docker build -t thingspanel-tdengine:1.0.0 . 
    注意：如果需要修改配置文件内容，请修改后重新build镜像
## 启动镜像
    docker run -it --name td thingspanel-tdengine:1.0.0
