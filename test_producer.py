"""
测试生产者 - 持续发送模拟数据
"""

import time
import random
from datetime import datetime
from kafka_consumer_realtime import KafkaRealtimeProducer

def generate_test_data():
    """生成测试数据"""
    skus = [f'SKU{i:03d}' for i in range(1, 11)]
    promotions = ['无活动', '全场8折', '满200减30', '限时秒杀']
    regions = ['华东', '华南', '华北', '西南']
    
    return {
        'sku': random.choice(skus),
        'date': datetime.now().strftime('%Y-%m-%d'),
        'sales': round(random.uniform(50, 200) + random.choice([0, 30, 60]), 2),
        'price': round(random.uniform(50, 150), 2),
        'inventory': random.randint(100, 1000),
        'promotion_text': random.choice(promotions),
        'region': random.choice(regions),
        'is_weekend': 1 if datetime.now().weekday() >= 5 else 0
    }

if __name__ == "__main__":
    print("="*60)
    print("Kafka测试生产者")
    print("按 Ctrl+C 停止")
    print("="*60)
    print("")
    
    producer = KafkaRealtimeProducer()
    count = 0
    
    try:
        while True:
            data = generate_test_data()
            producer.send_data(data)
            count += 1
            if count % 10 == 0:
                print(f"已发送 {count} 条数据")
            time.sleep(2)  # 每2秒发1条
    except KeyboardInterrupt:
        print(f"\n已停止，共发送 {count} 条数据")
        producer.close()