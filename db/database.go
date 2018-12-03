package db

type AkitaDb struct {
}



type Connection struct {
}



func (db *AkitaDb) Reload() (bool, error) {							   		//数据重新载入
	return false, nil
}

func (conn *Connection) Insert(key string, fileName string) (bool, error) {	//插入数据
	return false, nil
}

func (conn *Connection) Lookup(key string) ([]byte, error) {				//查找数据
	return nil, nil
}

func (conn *Connection) Delete(key string) (bool, error)  {					//删除数据
	return false,  nil
}

func (conn *Connection) Close() error {										//关闭连接
	return nil
}

