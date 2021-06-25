from pykafka import KafkaClient, SslConfig


cfg = SslConfig(cafile='truststore/ca-cert', certfile='keystore/combined.key', password='confluent')

client = KafkaClient(hosts="localhost:9093", ssl_config=cfg)

import pdb; pdb.set_trace()