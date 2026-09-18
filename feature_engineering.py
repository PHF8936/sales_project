"""
时间特征工程
把日期变成模型能理解的数字
"""

import pandas as pd
import numpy as np
from hdfs_client import HDFSClient

def create_time_features(df, date_column='date'):
    """
    将日期转换为数值特征
    
    为什么要这样做？
    - 模型只能理解数字，不能理解"2026-08-23"
    - 用sin/cos编码可以保留时间的周期性（如周一和周日相似）
    """
    
    print("开始时间特征工程...")
    
    # 确保日期格式正确
    df[date_column] = pd.to_datetime(df[date_column])
    
    # 特征1: 从第一天开始的天数（线性趋势）
    df['days_from_start'] = (df[date_column] - df[date_column].min()).dt.days
    
    # 特征2: 年度周期性（用sin/cos编码）
    df['day_of_year'] = df[date_column].dt.dayofyear
    df['sin_year'] = np.sin(2 * np.pi * df['day_of_year'] / 365)
    df['cos_year'] = np.cos(2 * np.pi * df['day_of_year'] / 365)
    
    # 特征3: 月度周期性
    df['day_of_month'] = df[date_column].dt.day
    df['sin_month'] = np.sin(2 * np.pi * df['day_of_month'] / 31)
    df['cos_month'] = np.cos(2 * np.pi * df['day_of_month'] / 31)
    
    # 特征4: 周周期性
    df['day_of_week'] = df[date_column].dt.dayofweek
    df['sin_week'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['cos_week'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    
    # 删除辅助列
    df = df.drop([date_column, 'day_of_year', 'day_of_month', 'day_of_week'], axis=1)
    
    print(f" 添加了 {len(df.columns) - 6} 个时间特征")
    print(f"现在的列: {df.columns.tolist()}")
    
    return df

def test_time_features():
    """测试时间特征工程"""
    print("\n测试时间特征工程...")
    
    # 加载数据
    # 1. 连接HDFS
    print("\n连接HDFS...")
    hdfs_client = HDFSClient()

    # 2. 读取最近30天的数据
    print("从HDFS读取最近30天数据...")
    df = hdfs_client.read_latest_data(days=30)  # 从HDFS读取
    # ============ 修改部分结束 ============

    if df.empty:
        print("没有数据，请先运行Kafka消费者")
        return

    print(f"读取了 {len(df)} 条数据")
    print(f"数据列: {df.columns.tolist()}")
    
    # 创建特征
    df_with_features = create_time_features(df)
    print(f"特征工程后形状: {df_with_features.shape}")
    
    # 查看前5行
    print("\n特征工程后的数据:")
    print(df_with_features.head())
    
    return df_with_features

if __name__ == "__main__":
    print("="*50)
    print("步骤2: 时间特征工程")
    print("="*50)
    test_time_features()