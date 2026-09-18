"""
配置文件 - 连接你的虚拟机基础设施
"""

# ============ Kafka配置 ============
KAFKA_CONFIG = {
    'bootstrap_servers': '192.168.1.101:9092',  # 比如 '192.168.1.100:9092'
    'topic_sales': 'sales_data',
    'topic_model': 'model_update',
    'group_id': 'sales_forecast_group',
    'auto_offset_reset': 'earliest'
}

# ============ HDFS配置 ============
HDFS_CONFIG = {
    'host': '192.168.1.101',  # 比如 '192.168.1.100'
    'port': 9870,           # NameNode端口（默认9870）
    'user': 'phf',       # HDFS用户
    'data_path': '/user/phf/sales_data',  # 数据存储路径
    'model_path': '/user/phf/models'      # 模型存储路径
}

# ============ Redis配置（可选） ============
REDIS_CONFIG = {
    'host': '192.168.1.101',
    'port': 6379,
    'db': 0
}

# ============ 训练配置 ============
TRAIN_CONFIG = {
    'batch_size': 1000,          # 每批训练数据量
    'retrain_interval': 3600,    # 重训间隔（秒）
    'min_data_for_train': 5000   # 最少数据量才触发训练
}