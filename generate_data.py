"""
生成模拟销售数据
运行这个文件会创建 sample_data.csv
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

print("="*50)
print("生成模拟销售数据")
print("="*50)

# 1. 生成日期（过去2年）
start_date = datetime(2024, 1, 1)
end_date = datetime(2026, 8, 23)
dates = pd.date_range(start=start_date, end=end_date, freq='D')
n = len(dates)

print(f"生成 {n} 天的数据")

# 2. 生成销量（带趋势和季节性）
trend = np.linspace(100, 200, n)  # 从100涨到200
seasonal = 50 * np.sin(2 * np.pi * np.arange(n) / 365)  # 年度周期
noise = np.random.normal(0, 20, n)  # 随机波动
sales = trend + seasonal + noise

# 3. 生成其他特征
np.random.seed(42)
data = pd.DataFrame({
    'date': dates,
    'sales': sales,
    'price': np.random.uniform(50, 150, n),
    'inventory': np.random.randint(100, 1000, n),
    'is_weekend': (dates.dayofweek >= 5).astype(int),
    'promotion_text': np.random.choice([
        '全场8折', '满200减30', '限时秒杀', '新品上市', '无活动'
    ], n)
})

# 4. 加入促销效应
promotion_boost = data['promotion_text'].map({
    '全场8折': 50,
    '满200减30': 30,
    '限时秒杀': 80,
    '新品上市': 20,
    '无活动': 0
})
data['sales'] = data['sales'] + promotion_boost

# 5. 保存
data.to_csv('data/sample_data.csv', index=False)

print(f" 数据已保存到 data/sample_data.csv")
print(f"数据预览:")
print(data.head())