### Quick Start

#### download

```
go get -u github.com/DomBro96/akita
                or
git clone https://github.com/DomBro96/akita.git
```

#### before

```
fill in the configuration file $GOPATH/src/github.com/akita/conf/akita.ini
```

#### install

```
go build -o akita
```

#### start server

```
1. cd $GOPATH/src/github.com/akita/
2. ./akita
```

#### stop server (Temporary plan)

```
kill -9 pid
    or
ctrl + c
```

#### insert

```
curl -X POST "http://master_intranet_ip:port/akita/save" -F "file=@picture_path" -F "key=key1"
```

#### seek

```
curl -X GET "http://master_or_slave_intranet_ip:port/akita/seek?key=key1"
```

#### del

```
curl -X GET "http://master_intranet_ip:port/akita/del?key=key1"
```
