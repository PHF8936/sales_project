"""
归因分析 - 解释"为什么预测这个数"
"""

import pandas as pd
import numpy as np
import joblib
from feature_engineering import create_time_features


class SimpleAttribution:
    """
    简单的归因分析器

    作用：解释某个预测结果受哪些因素影响最大
    """

    def __init__(self):
        """加载模型和数据"""
        print(" 初始化归因分析器...")

        self.model = joblib.load('models/model.pkl')
        self.scaler = joblib.load('models/scaler.pkl')
        self.feature_names = joblib.load('models/feature_names.pkl')

        # 加载并处理数据
        df = pd.read_csv('data/sample_data.csv')
        df = create_time_features(df)
        df = pd.get_dummies(df, columns=['promotion_text'], prefix='promo')
        self.X = df.select_dtypes(include=[np.number])

        # 确保列一致
        for col in self.feature_names:
            if col not in self.X.columns:
                self.X[col] = 0
        self.X = self.X[self.feature_names]

        self.y = df['sales']

        print("归因分析器初始化完成")

    def explain_one(self, index):
        """
        解释第 index 条数据的预测

        方法：逐个特征打乱，看预测变化多大
        """

        # 1. 获取该样本
        X_scaled = self.scaler.transform(self.X.values)
        sample = X_scaled[index:index + 1]

        # 2. 原始预测
        base_pred = self.model.predict(sample)[0]
        actual = self.y.iloc[index]

        # 3. 计算每个特征的影响
        impacts = []
        for i, feat_name in enumerate(self.feature_names):
            # 复制样本
            sample_modified = sample.copy()
            # 给这个特征加噪声（模拟特征变化）
            sample_modified[0][i] = sample_modified[0][i] + np.random.normal(0, 0.5)
            # 重新预测
            new_pred = self.model.predict(sample_modified)[0]
            # 记录影响
            impact = abs(new_pred - base_pred)
            impacts.append((feat_name, impact))

        # 4. 排序，取前5
        impacts.sort(key=lambda x: x[1], reverse=True)
        top5 = impacts[:5]

        # 5. 生成报告
        report = f"""
 归因分析报告 (样本 {index})
{'=' * 50}
实际销量: {actual:.2f}
预测销量: {base_pred:.2f}
偏差: {actual - base_pred:.2f}

主要影响因素 (Top 5):
"""
        for i, (name, impact) in enumerate(top5, 1):
            report += f"  {i}. {name}: 影响程度 {impact:.4f}\n"

        report += f"\n结论: "
        if base_pred > actual:
            report += f"预测值高于实际值 {base_pred - actual:.2f}"
        else:
            report += f"预测值低于实际值 {actual - base_pred:.2f}"

        return report, top5


def test_attribution():
    """测试归因分析"""
    print("=" * 50)
    print("步骤4: 归因分析测试")
    print("=" * 50)

    # 创建归因分析器
    attr = SimpleAttribution()

    # 解释第100条数据
    print("\n 分析样本 100:")
    report, factors = attr.explain_one(100)
    print(report)

    # 解释第500条数据
    print("\n 分析样本 500:")
    report, factors = attr.explain_one(500)
    print(report)


if __name__ == "__main__":
    test_attribution()