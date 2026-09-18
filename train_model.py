"""
训练预测模型
用最简单的线性回归，让你看到效果
"""

import pandas as pd 
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
import joblib
from feature_engineering import create_time_features

def train_model():
    """
     训练销售预测模型
    步骤：
    1. 加载数据
    2. 特征工程
    3. 准备训练数据
    4. 训练模型
    5. 评估模型
    6. 保存模型
    """

    print("=" * 50)
    print("步骤3: 训练预测模型")
    print("=" * 50)

    # 1. 加载数据
    print("\n加载数据...")
    df = pd.read_csv('data/sample_data.csv')
    print(f"加载了 {len(df)} 条数据")

    # 2. 特征工程
    print("\n特征工程...")
    df = create_time_features(df)

    # 3. 处理文本特征（one-hot编码）
    print("\n处理文本特征...")
    df = pd.get_dummies(df, columns=['promotion_text'], prefix='promo')
    print(f"处理后列数: {len(df.columns)}")

    # 4. 准备特征X和目标y
    print("\n准备训练数据...")
    X = df.drop('sales', axis=1)
    y = df['sales']

    # 只保留数值列
    X = X.select_dtypes(include=[np.number])
    print(f"特征数量: {X.shape[1]}")

    # 5. 划分训练集和测试集（按时间顺序）
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    print(f"训练集: {len(X_train)} 条")
    print(f"测试集: {len(X_test)} 条")

    # 6. 标准化（让所有特征在同一尺度）
    print("\n标准化特征...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # 7. 训练模型
    print("\n训练模型...")
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)
    print("训练完成")

    # 8. 预测和评估
    print("\n评估模型...")
    y_pred = model.predict(X_test_scaled)

    mape = mean_absolute_percentage_error(y_test, y_pred) * 100
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    print(f"  - MAPE (平均绝对百分比误差): {mape:.2f}%")
    print(f"  - RMSE (均方根误差): {rmse:.2f}")

    # 9. 显示特征重要性（线性回归的系数）
    print("\n最重要的特征:")
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': abs(model.coef_)
    })
    feature_importance = feature_importance.sort_values('importance', ascending=False)
    print(feature_importance.head(10))

    # 10. 保存模型
    print("\n保存模型...")
    joblib.dump(model, 'models/model.pkl')
    joblib.dump(scaler, 'models/scaler.pkl')
    joblib.dump(X.columns.tolist(), 'models/feature_names.pkl')
    print("模型已保存")

    return model, scaler


def test_prediction():
    """测试预测功能"""
    print("\n" + "=" * 50)
    print("测试预测")
    print("=" * 50)

    # 加载模型
    model = joblib.load('models/model.pkl')
    scaler = joblib.load('models/scaler.pkl')
    feature_names = joblib.load('models/feature_names.pkl')

    # 加载最新数据预测
    df = pd.read_csv('data/sample_data.csv')
    df = create_time_features(df)
    df = pd.get_dummies(df, columns=['promotion_text'], prefix='promo')
    X = df.select_dtypes(include=[np.number])

    # 确保列一致
    for col in feature_names:
        if col not in X.columns:
            X[col] = 0
    X = X[feature_names]

    # 预测最后一天
    last_row = X.iloc[-1:].values
    last_row_scaled = scaler.transform(last_row)
    pred = model.predict(last_row_scaled)[0]
    actual = df['sales'].iloc[-1]

    print(f"最后一天预测:")
    print(f"  实际销量: {actual:.2f}")
    print(f"  预测销量: {pred:.2f}")
    print(f"  误差: {abs(pred - actual):.2f}")


if __name__ == "__main__":
    # 训练模型
    model, scaler = train_model()

    # 测试预测
    test_prediction()