--创建表语句(采用hash进行预分区)
create 't_status_004', { NAME => 'info', COMPRESSION => 'snappy' },  {NUMREGIONS => 2, SPLITALGO => 'HexStringSplit'}
