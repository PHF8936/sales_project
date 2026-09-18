"""
Kafka消费者 - 生产模式
持续消费Kafka数据并写入HDFS
按 Ctrl+C 停止
"""

import json
import time
import threading
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
from datetime import datetime
import logging
from config import KAFKA_CONFIG
from hdfs_client import HDFSClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaRealtimeConsumer:
    """
    连接Kafka集群，实时消费销售数据，写入HDFS
    """
    
    def __init__(self):
        self.bootstrap_servers = KAFKA_CONFIG['bootstrap_servers']
        self.topic = KAFKA_CONFIG['topic_sales']
        self.group_id = KAFKA_CONFIG['group_id']
        
        # HDFS客户端
        self.hdfs = HDFSClient()
        
        # 数据缓冲区
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.batch_size = 100  # 每100条写一次HDFS
        
        # 计数器
        self.total_count = 0
        
        # Kafka消费者
        self.consumer = None
        self.running = False
        
        logger.info(f"Kafka消费者初始化完成")
        logger.info(f"   Broker: {self.bootstrap_servers}")
        logger.info(f"   Topic: {self.topic}")
        logger.info(f"   批次大小: {self.batch_size} 条")
    
    def connect(self):
        """连接Kafka"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=60000,
                max_poll_records=500
            )
            logger.info("Kafka连接成功")
            return True
        except Exception as e:
            logger.error(f"Kafka连接失败: {e}")
            return False
    
    def process_message(self, message):
        """处理单条消息"""
        data = message.value
        
        # 添加接收时间
        if 'timestamp' not in data:
            data['receive_time'] = datetime.now().isoformat()
        
        # 更新计数
        self.total_count += 1
        
        # 每收到一条都打印（方便观察）
        sku = data.get('sku', 'unknown')
        sales = data.get('sales', 0)
        print(f"[{self.total_count}] 收到: {sku} -> 销量: {sales}")
        
        # 加入缓冲区
        with self.buffer_lock:
            self.buffer.append(data)
            
            # 批量写入HDFS
            if len(self.buffer) >= self.batch_size:
                self.flush_buffer()
    
    def flush_buffer(self):
        """将缓冲区数据写入HDFS"""
        with self.buffer_lock:
            if not self.buffer:
                return
            
            # 转为DataFrame
            df = pd.DataFrame(self.buffer)
            
            # 按日期分区
            if 'date' in df.columns:
                date = df['date'].iloc[0]
                partition = f"dt={date}"
            else:
                date = datetime.now().strftime('%Y-%m-%d')
                partition = f"dt={date}"
            
            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"batch_{timestamp}.csv"
            
            # 保存到HDFS
            self.hdfs.save_dataframe(df, filename, partition)
            
            print(f"写入HDFS: {len(self.buffer)} 条 (累计 {self.total_count} 条)")
            self.buffer = []
    
    def start(self):
        """启动消费（阻塞）"""
        if not self.connect():
            logger.error("无法连接Kafka，退出")
            return
        
        self.running = True
        print("开始监听Kafka消息...")
        print("每收到一条数据都会显示")
        print("每100条写入一次HDFS")
        print("")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                self.process_message(message)
        except KeyboardInterrupt:
            print("用户中断")
        except Exception as e:
            logger.error(f"消费异常: {e}")
        finally:
            self.flush_buffer()
            if self.consumer:
                self.consumer.close()
            print("Kafka消费者已关闭")
    
    def stop(self):
        """停止消费"""
        self.running = False


class KafkaRealtimeProducer:
    """Kafka生产者 - 用于发送数据"""
    
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = KAFKA_CONFIG['topic_sales']
    
    def send_data(self, data: dict):
        """发送单条数据"""
        try:
            future = self.producer.send(self.topic, value=data)
            result = future.get(timeout=10)
            print(f"发送成功: {data.get('sku')} - {data.get('sales')}")
            return True
        except Exception as e:
            logger.error(f"发送失败: {e}")
            return False
    
    def send_batch(self, data_list: list):
        """批量发送"""
        for data in data_list:
            self.send_data(data)
    
    def close(self):
        self.producer.close()


if __name__ == "__main__":
    print("="*60)
    print("Kafka消费者 - 生产模式")
    print("按 Ctrl+C 停止")
    print("="*60)
    print("")
    
    consumer = KafkaRealtimeConsumer()
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        print("\n正在关闭...")
        consumer.stop()
        print("已关闭")