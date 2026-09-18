"""
从HDFS读取数据训练模型
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_percentage_error
import joblib
from hdfs_client import HDFSClient
from feature_engineering import create_time_features


def train_from_hdfs(days: int = 30):
    """
    从HDFS读取最近N天数据训练
    
    Args:
        days: 读取最近多少天的数据
    """
    print("="*50)
    print("从HDFS读取数据训练模型")
    print("="*50)
    
    # 1. 连接HDFS
    hdfs = HDFSClient()
    
    # 2. 读取最近数据
    print(f"读取最近 {days} 天的数据...")
    df = hdfs.read_latest_data(days)
    
    if df.empty:
        print("没有数据，请先运行Kafka消费者")
        return None, None
    
    print(f"读取了 {len(df)} 条数据")
    
    # 3. 特征工程
    print("特征工程...")
    df = create_time_features(df)
    df = pd.get_dummies(df, columns=['promotion_text'], prefix='promo')
    
    print("处理缺失值...")
    print(f"处理前数据量: {len(df)}")
    
    # 检查缺失值
    missing_cols = df.columns[df.isnull().any()].tolist()
    if missing_cols:
        print(f"发现缺失值的列: {missing_cols}")
        # 查看每列缺失数量
        for col in missing_cols:
            print(f"  {col}: {df[col].isnull().sum()} 个缺失")
    
    # 方法1：删除包含NaN的行（数据量小的时候）
    df = df.dropna()
    print(f"删除缺失值后数据量: {len(df)}")
    
    # 方法2：填充缺失值（如果数据量大，可以用均值/中位数填充）
    # df = df.fillna(df.mean())
    
    if len(df) == 0:
        print("删除缺失值后没有数据了，请检查数据质量")
        return None, None

    # 4. 准备数据
    X = df.drop('sales', axis=1)
    X = X.select_dtypes(include=[np.number])
    y = df['sales']
    
    # 5. 划分训练集
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    # 6. 标准化
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # 7. 训练
    print("训练模型...")
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)
    
    # 8. 评估
    y_pred = model.predict(X_test_scaled)
    mape = mean_absolute_percentage_error(y_test, y_pred) * 100
    
    print(f"评估: MAPE = {mape:.2f}%")
    
    # 9. 保存模型到HDFS
    print("保存模型到HDFS...")
    hdfs.save_model(model, 'sales_model')
    hdfs.save_model(scaler, 'scaler')
    
    print("训练完成！")
    return model, scaler


if __name__ == "__main__":
    train_from_hdfs(days=30)