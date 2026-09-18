"""
实时数据仪表板 - 可视化Kafka消费数据
访问 http://localhost:8050 查看
"""

import json
import threading
import time
from datetime import datetime, timedelta
from collections import deque
import pandas as pd

from dash import Dash, html, dcc, Input, Output
import plotly.graph_objs as go
from flask import Flask
from flask_socketio import SocketIO

from hdfs_client import HDFSClient
from kafka_consumer_realtime import KafkaRealtimeConsumer

# ============================================
# 配置
# ============================================
MAX_POINTS = 1000  # 图表最多显示100个点
REFRESH_INTERVAL = 2  # 刷新间隔（秒）

# ============================================
# 数据存储（内存）
# ============================================
class RealtimeDataStore:
    """存储实时数据"""
    
    def __init__(self):
        self.timestamps = deque(maxlen=MAX_POINTS)
        self.sales_values = deque(maxlen=MAX_POINTS)
        self.sku_counts = {}
        self.region_counts = {}
        self.total_count = 0
        self.latest_data = None
    
    def add_data(self, data):
        """添加一条数据"""
        self.total_count += 1
        
        # 时间序列数据
        if 'sales' in data:
            self.timestamps.append(datetime.now())
            self.sales_values.append(float(data['sales']))
        
        # SKU统计
        sku = data.get('sku', 'unknown')
        self.sku_counts[sku] = self.sku_counts.get(sku, 0) + 1
        
        # 区域统计
        region = data.get('region', 'unknown')
        self.region_counts[region] = self.region_counts.get(region, 0) + 1
        
        self.latest_data = data
    
    def get_stats(self):
        """获取统计信息"""
        return {
            'total': self.total_count,
            'sku_count': len(self.sku_counts),
            'latest_sales': self.sales_values[-1] if self.sales_values else 0,
            'avg_sales': sum(self.sales_values) / len(self.sales_values) if self.sales_values else 0,
            'max_sales': max(self.sales_values) if self.sales_values else 0,
            'top_sku': max(self.sku_counts.items(), key=lambda x: x[1])[0] if self.sku_counts else 'N/A'
        }

# 全局数据存储
store = RealtimeDataStore()

# ============================================
# Kafka消费者（后台线程）
# ============================================
class RealtimeConsumerThread(threading.Thread):
    """后台消费Kafka数据"""
    
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.running = True
        self.consumer = None
    
    def run(self):
        from kafka import KafkaConsumer
        import json
        from config import KAFKA_CONFIG
        
        print("启动Kafka消费者后台线程...")
        
        try:
            self.consumer = KafkaConsumer(
                KAFKA_CONFIG['topic_sales'],
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                group_id='dashboard_group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=5000
            )
            print("Kafka连接成功，开始监听...")

            print(f"订阅的topic: {self.consumer.subscription()}")
            print(f"已分配的partition: {self.consumer.assignment()}")

            for message in self.consumer:
                if not self.running:
                    break
                data = message.value
                store.add_data(data)
                print(f"收到: {data.get('sku')} - 销量: {data.get('sales')}")
                
        except Exception as e:
            print(f"Kafka消费错误: {e}")
    
    def stop(self):
        self.running = False
        if self.consumer:
            self.consumer.close()

# 启动后台消费者
consumer_thread = RealtimeConsumerThread()
consumer_thread.start()

# ============================================
# Dash 仪表板
# ============================================
app = Dash(__name__, title="实时销售监控")

app.layout = html.Div([
    html.H1("实时销售数据监控", style={'textAlign': 'center'}),
    
    # 统计卡片
    html.Div([
        html.Div([
            html.H4("总数据量", style={'margin': '0'}),
            html.H2(id='total-count', children='0', style={'color': '#007bff'})
        ], className='stat-card', style={'display': 'inline-block', 'width': '20%', 'margin': '10px', 'padding': '15px', 'border': '1px solid #ddd', 'borderRadius': '8px', 'textAlign': 'center'}),
        
        html.Div([
            html.H4("最新销量", style={'margin': '0'}),
            html.H2(id='latest-sales', children='0', style={'color': '#28a745'})
        ], className='stat-card', style={'display': 'inline-block', 'width': '20%', 'margin': '10px', 'padding': '15px', 'border': '1px solid #ddd', 'borderRadius': '8px', 'textAlign': 'center'}),
        
        html.Div([
            html.H4("平均销量", style={'margin': '0'}),
            html.H2(id='avg-sales', children='0', style={'color': '#ffc107'})
        ], className='stat-card', style={'display': 'inline-block', 'width': '20%', 'margin': '10px', 'padding': '15px', 'border': '1px solid #ddd', 'borderRadius': '8px', 'textAlign': 'center'}),
        
        html.Div([
            html.H4("SKU种类", style={'margin': '0'}),
            html.H2(id='sku-count', children='0', style={'color': '#17a2b8'})
        ], className='stat-card', style={'display': 'inline-block', 'width': '20%', 'margin': '10px', 'padding': '15px', 'border': '1px solid #ddd', 'borderRadius': '8px', 'textAlign': 'center'}),
        
        html.Div([
            html.H4("热门SKU", style={'margin': '0'}),
            html.H2(id='top-sku', children='N/A', style={'color': '#dc3545'})
        ], className='stat-card', style={'display': 'inline-block', 'width': '20%', 'margin': '10px', 'padding': '15px', 'border': '1px solid #ddd', 'borderRadius': '8px', 'textAlign': 'center'}),
    ], style={'textAlign': 'center'}),
    
    # 图表
    dcc.Graph(id='sales-trend', style={'height': '400px'}),
    dcc.Graph(id='sales-distribution', style={'height': '400px'}),
    
    # 最新数据
    html.Div([
        html.H4("最新收到的数据", style={'margin': '10px'}),
        html.Pre(id='latest-data-display', style={'background': '#f8f9fa', 'padding': '15px', 'borderRadius': '8px', 'fontSize': '12px', 'maxHeight': '200px', 'overflow': 'auto'})
    ], style={'margin': '20px'}),
    
    # 刷新定时器
    dcc.Interval(
        id='interval-component',
        interval=REFRESH_INTERVAL * 1000,
        n_intervals=0
    )
])

# ============================================
# 回调函数
# ============================================
@app.callback(
    [
        Output('total-count', 'children'),
        Output('latest-sales', 'children'),
        Output('avg-sales', 'children'),
        Output('sku-count', 'children'),
        Output('top-sku', 'children'),
        Output('sales-trend', 'figure'),
        Output('sales-distribution', 'figure'),
        Output('latest-data-display', 'children')
    ],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n):
    """更新仪表板"""
    
    # 获取统计
    stats = store.get_stats()
    
    # 1. 趋势图
    trend_fig = go.Figure()
    if store.sales_values:
        trend_fig.add_trace(go.Scatter(
            x=list(store.timestamps),
            y=list(store.sales_values),
            mode='lines+markers',
            name='销量',
            line=dict(color='#007bff', width=2)
        ))
    trend_fig.update_layout(
        title='销量实时趋势',
        xaxis_title='时间',
        yaxis_title='销量',
        template='plotly_white'
    )
    
    # 2. 分布图
    dist_fig = go.Figure()
    if store.region_counts:
        dist_fig.add_trace(go.Bar(
            x=list(store.region_counts.keys()),
            y=list(store.region_counts.values()),
            marker_color='#17a2b8'
        ))
    dist_fig.update_layout(
        title='区域销量分布',
        xaxis_title='区域',
        yaxis_title='订单数',
        template='plotly_white'
    )
    
    # 3. 最新数据
    latest_display = json.dumps(store.latest_data, indent=2, default=str) if store.latest_data else "暂无数据"
    
    return (
        str(stats['total']),
        f"{stats['latest_sales']:.1f}",
        f"{stats['avg_sales']:.1f}",
        str(stats['sku_count']),
        stats['top_sku'],
        trend_fig,
        dist_fig,
        latest_display
    )

# ============================================
# 启动
# ============================================
if __name__ == '__main__':
    print("="*60)
    print("实时销售监控仪表板")
    print("访问: http://localhost:8050")
    print("按 Ctrl+C 停止")
    print("="*60)
    
    app.run(debug=True, host='0.0.0.0', port=8050)