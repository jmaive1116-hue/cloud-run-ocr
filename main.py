import os
import json
import time
from flask import Flask, jsonify, request
from google.cloud import storage, documentai_v1 as documentai

# 初始化 Flask 应用
app = Flask(__name__)

# -----------------------------------------------
# 配置信息 (从环境变量中获取，确保 Cloud Run 部署时设置了这些变量)
# -----------------------------------------------
try:
    PROJECT_ID = os.environ["PROJECT_ID"]
    LOCATION = os.environ["PROCESSOR_LOCATION"]  # 例如: "us" 或 "eu"
    PROCESSOR_ID = os.environ["PROCESSOR_ID"]
    INPUT_BUCKET = os.environ["INPUT_BUCKET"]
    OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
    INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "") # 可选：如 "papers/"
    # 默认的输出路径前缀
    OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "doc_ai_batch_output/")
except KeyError as e:
    print(f"Error: Missing required environment variable: {e}")
    # 在生产环境中，这应该导致容器启动失败
    PROJECT_ID, LOCATION, PROCESSOR_ID, INPUT_BUCKET, OUTPUT_BUCKET = "", "", "", "", ""


# -----------------------------------------------
# Document AI 异步批量处理核心函数
# -----------------------------------------------
def run_batch_document_ai_async():
    """
    启动 Document AI 批量处理任务，对 GCS 输入路径下的所有 PDF 文件进行 OCR。
    """
    if not all([PROJECT_ID, LOCATION, PROCESSOR_ID, INPUT_BUCKET, OUTPUT_BUCKET]):
        raise EnvironmentError("One or more required environment variables are not set.")

    # 构造 GCS 路径
    gcs_input_prefix = f"gs://{INPUT_BUCKET}/{INPUT_PREFIX}"
    gcs_output_prefix = f"gs://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}"
    
    # 初始化 Document AI 客户端
    client = documentai.DocumentProcessorServiceClient()
    name = client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    # 1. 设置输入配置：指向 GCS 前缀，处理所有 PDF
    input_config = documentai.BatchProcessRequest.BatchInputConfig(
        gcs_prefix=gcs_input_prefix, 
        mime_type="application/pdf"
    )

    # 2. 设置输出配置：Document AI 将把结果写入此路径
    output_config = documentai.BatchProcessRequest.BatchOutputConfig(
        gcs_output_uri=gcs_output_prefix
    )

    request = documentai.BatchProcessRequest(
        name=name,
        input_documents=input_config,
        document_output_config=output_config
    )

    # 3. 异步调用批量处理 API (返回 Long Running Operation)
    # 任务将在 Google Cloud 后台运行，不占用 Cloud Run 资源
    operation = client.batch_process_documents(request=request)

    print(f"Batch processing started. Operation name: {operation.operation.name}")
    
    return operation.operation.name, gcs_output_prefix

# -----------------------------------------------
# HTTP 端点
# -----------------------------------------------

@app.route("/", methods=["GET"])
def health_check():
    """用于 Cloud Run 健康检查"""
    return "Batch Document AI Service is ready.", 200

@app.route("/run-batch", methods=["GET", "POST"])
def run_batch():
    """
    触发异步 Document AI 批量处理任务。
    """
    try:
        operation_name, output_path = run_batch_document_ai_async()
        
        # 返回 202 Accepted，表示任务已接受并在后台处理
        return jsonify({
            "status": "Batch processing accepted and running asynchronously.",
            "operation_name": operation_name,
            "input_path": f"gs://{INPUT_BUCKET}/{INPUT_PREFIX}",
            "output_path": output_path,
            "monitor_url": f"https://console.cloud.google.com/operations/detail/{operation_name.split('/')[-1]}?project={PROJECT_ID}"
        }), 202

    except EnvironmentError as e:
        return jsonify({"error": str(e), "message": "Check Cloud Run environment variables."}), 500
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return jsonify({"error": "Failed to start batch process.", "details": str(e)}), 500


# -----------------------------------------------
# Cloud Run 启动入口
# -----------------------------------------------
if __name__ == "__main__":
    # Cloud Run 通常使用 Gunicorn 或 uWSGI 启动，但对于本地测试，使用 app.run
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
