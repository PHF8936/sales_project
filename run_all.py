"""
运行完整流程
从数据生成到API服务
"""

import subprocess
import time
import sys


def print_step(step, message):
    print(f"\n{'=' * 60}")
    print(f"步骤 {step}: {message}")
    print('=' * 60)


def run_command(cmd, description):
    """运行命令并检查结果"""
    print(f"\n️ {description}")
    print(f"命令: {cmd}")

    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode == 0:
        print(" 成功")
        if result.stdout:
            print(result.stdout[-500:])  # 显示最后500字符
    else:
        print(" 失败")
        print(result.stderr)
        return False

    return True


def main():
    print("销售预测平台 - 完整流程")
    print("=" * 60)

    # 检查环境
    print("\n检查环境...")
    try:
        import pandas, numpy, sklearn, fastapi, uvicorn
        print("所有依赖已安装")
    except ImportError as e:
        print(f"缺少依赖: {e}")
        print("请运行: pip install -r requirements.txt")
        return

    # 步骤1: 生成数据
    if not run_command("python generate_data.py", "生成模拟数据"):
        return

    # 步骤2: 特征工程
    if not run_command("python feature_engineering.py", "时间特征工程"):
        return

    # 步骤3: 训练模型
    if not run_command("python train_model.py", "训练预测模型"):
        return

    # 步骤4: 归因分析
    if not run_command("python attribution.py", "归因分析测试"):
        return

    # 步骤5: 启动API（后台运行）
    print("\n️ 启动API服务")
    import subprocess
    import threading

    def run_api():
        subprocess.run("python start_api.py", shell=True)

    api_thread = threading.Thread(target=run_api)
    api_thread.daemon = True
    api_thread.start()

    print(" 等待API启动...")
    time.sleep(3)

    # 测试API
    print("\n️ 测试API")
    import requests
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            print("API服务正常")
            print(f"   响应: {response.json()}")
        else:
            print(f" API异常: {response.status_code}")
    except Exception as e:
        print(f" 无法连接API: {e}")
        print("请手动运行: python start_api.py")

    # 完成
    print("\n" + "=" * 60)
    print(" 所有步骤完成！")
    print("=" * 60)
    print("\n现在你可以:")
    print("  1. 访问 http://localhost:8000 查看API")
    print("  2. 访问 http://localhost:8000/docs 查看文档")
    print("  3. 测试预测: http://localhost:8000/predict?date=2026-08-23")
    print("  4. 查看归因: python attribution.py")
    print("\n按 Ctrl+C 停止API服务")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n 用户中断")
        sys.exit(0)