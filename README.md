### Quick Start

#### before

```
fill in the configuration file $GOPATH/src/github.com/akita/conf/akita.ini
```

#### install

```
1. go get -u github.com/DomBro96/akita or git clone https://github.com/DomBro96/akita.git
2. go build -o akita
```

#### start server

```
1. cd $GOPATH/src/github.com/akita/
2. ./akita
```

#### stop server

```
kill -9 pid or ctrl + c
```

#### insert

```
curl -X POST "http://master_intranet_ip:port/save" -F "file=@picture_path" -F "key=key1"
```

#### seek

```
curl -X GET "http://master_or_slave_intranet_ip:port/seek?key=key1"
```

#### del

```
curl -X GET "http://master_intranet_ip:port/del?key=key1"
```
