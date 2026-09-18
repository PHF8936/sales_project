"""
启动API服务器
"""

import uvicorn

if __name__ == "__main__":
    print("=" * 50)
    print("启动销售预测API")
    print("=" * 50)
    print("\n服务启动中...")
    print("访问地址: http://localhost:8000")
    print("API文档: http://localhost:8000/docs")
    print("️按 Ctrl+C 停止\n")

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )