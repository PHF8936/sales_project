"""
HDFS客户端 - 读写大数据
"""

import pandas as pd
import io
import json
from hdfs import InsecureClient
from hdfs.util import HdfsError
import logging
from config import HDFS_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HDFSClient:
    """
    HDFS客户端封装
    支持：读写CSV、JSON、保存/加载模型
    """
    
    def __init__(self, host=None, port=None, user=None):
        self.host = host or HDFS_CONFIG['host']
        self.port = port or HDFS_CONFIG['port']
        self.user = user or HDFS_CONFIG['user']
        
        # 创建客户端
        self.client = InsecureClient(
            f'http://{self.host}:{self.port}',
            user='phf'
        )
        
        # 确保目录存在
        self._ensure_dirs()
        
        logger.info(f"HDFS连接成功: {self.host}:{self.port}")
    
    def _ensure_dirs(self):
        """确保数据目录存在"""
        for path in [HDFS_CONFIG['data_path'], HDFS_CONFIG['model_path']]:
            try:
                if not self.client.status(path, strict=False):
                    self.client.makedirs('/user/phf/salfs_data')
                    self.client.makedirs('/user/phf/models')
                    logger.info(f"创建目录: {path}")
            except Exception as e:
                logger.warning(f"目录检查失败: {e}")
    
    def save_dataframe(self, df: pd.DataFrame, filename: str, partition: str = None):
        """
        保存DataFrame到HDFS
        
        Args:
            df: 要保存的数据
            filename: 文件名（如 'sales_20260901.csv'）
            partition: 分区名（如 'dt=2026-09-01'）
        """
        # 构建路径
        base_path = HDFS_CONFIG['data_path']
        if partition:
            path = f"{base_path}/{partition}/{filename}"
        else:
            path = f"{base_path}/{filename}"
        
        # 转成CSV字符串
        csv_data = df.to_csv(index=False)
        
        # 写入HDFS
        with self.client.write(path, encoding='utf-8') as writer:
            writer.write(csv_data)
        
        logger.info(f"数据已保存到HDFS: {path} ({len(df)} 条)")
        return path
    
    def read_dataframe(self, path: str) -> pd.DataFrame:
        """从HDFS读取DataFrame"""
        try:
            with self.client.read(path, encoding='utf-8') as reader:
                df = pd.read_csv(reader)
            logger.info(f"从HDFS读取数据: {path} ({len(df)} 条)")
            return df
        except Exception as e:
            logger.error(f"读取失败: {e}")
            return pd.DataFrame()
    
    def read_latest_data(self, days: int = 30) -> pd.DataFrame:
        """
        读取最近N天的数据（按分区读取）
        """
        from datetime import datetime, timedelta
        
        base_path = HDFS_CONFIG['data_path']
        all_data = []
        
        # 读取最近N天的分区
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            partition = f"dt={date}"
            path = f"{base_path}/{partition}"
            
            try:
                # 列出该分区下的所有CSV
                files = self.client.list(path)
                for file in files:
                    if file.endswith('.csv'):
                        df = self.read_dataframe(f"{path}/{file}")
                        if not df.empty:
                            all_data.append(df)
            except:
                continue
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            logger.info(f"读取了 {len(result)} 条最近数据")
            return result
        else:
            return pd.DataFrame()
    
    def save_model(self, model, name: str):
        """
        保存模型到HDFS
        """
        import joblib
        import io
        
        path = f"{HDFS_CONFIG['model_path']}/{name}.pkl"
        
        # 序列化模型
        buffer = io.BytesIO()
        joblib.dump(model, buffer)
        buffer.seek(0)
        
        # 写入HDFS
        with self.client.write(path, overwrite=True) as writer:
            writer.write(buffer.getvalue())
        
        logger.info(f"模型已保存到HDFS: {path}")
        return path
    
    def load_model(self, name: str):
        """
        从HDFS加载模型
        """
        import joblib
        import io
        
        path = f"{HDFS_CONFIG['model_path']}/{name}.pkl"
        
        try:
            with self.client.read(path) as reader:
                data = reader.read()
                model = joblib.load(io.BytesIO(data))
            logger.info(f"模型已加载: {path}")
            return model
        except Exception as e:
            logger.error(f"加载模型失败: {e}")
            return None
    
    def list_files(self, path: str = None):
        """列出HDFS文件"""
        path = path or HDFS_CONFIG['data_path']
        try:
            files = self.client.list(path)
            logger.info(f"{path} 下的文件: {files}")
            return files
        except Exception as e:
            logger.error(f"列表失败: {e}")
            return []


if __name__ == "__main__":
    # 测试HDFS连接
    print("测试HDFS连接...")
    client = HDFSClient()
    
    # 测试写入
    import pandas as pd
    import numpy as np
    
    test_df = pd.DataFrame({
        'sku': ['SKU001', 'SKU002'],
        'sales': [100, 200],
        'timestamp': pd.Timestamp.now()
    })
    
    client.save_dataframe(test_df, 'test.csv')
    
    # 测试读取
    read_df = client.read_dataframe(f"{HDFS_CONFIG['data_path']}/test.csv")
    print(read_df)