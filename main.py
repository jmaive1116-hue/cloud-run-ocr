import os
import json
from flask import Flask, jsonify
from google.cloud import documentai_v1 as documentai

app = Flask(__name__)

# -------------------------
# 环境变量
# -------------------------
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("PROCESSOR_LOCATION")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")

INPUT_BUCKET = os.getenv("INPUT_BUCKET")
INPUT_PREFIX = os.getenv("INPUT_PREFIX", "")

OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "docai_batch_output/")


# -------------------------
# 批处理函数（最新 API）
# -------------------------
def run_batch_document_ai_async():

    client = documentai.DocumentProcessorServiceClient()
    name = client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    # ====== 输入配置：前缀方式 ======
    gcs_prefix = documentai.GcsPrefix(
        gcs_uri_prefix=f"gs://{INPUT_BUCKET}/{INPUT_PREFIX}"
    )

    input_config = documentai.BatchDocumentsInputConfig(
        gcs_prefix=gcs_prefix
    )

    # ====== 输出配置 ======
    gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
        gcs_uri=f"gs://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}"
    )

    output_config = documentai.DocumentOutputConfig(
        gcs_output_config=gcs_output_config
    )

    # ====== 批处理请求 ======
    request = documentai.BatchProcessRequest(
        name=name,
        input_documents=input_config,
        document_output_config=output_config
    )

    operation = client.batch_process_documents(request)
    print("Batch operation started:", operation.operation.name)

    return operation.operation.name, f"gs://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}"


# -------------------------
# REST API
# -------------------------
@app.route("/", methods=["GET"])
def health():
    return "Batch OCR service is running.", 200


@app.route("/run-batch", methods=["GET", "POST"])
def run_batch():
    try:
        operation_name, output_path = run_batch_document_ai_async()
        return jsonify({
            "status": "started",
            "operation_name": operation_name,
            "input_prefix": f"gs://{INPUT_BUCKET}/{INPUT_PREFIX}",
            "output_prefix": output_path,
            "monitor_url": (
                f"https://console.cloud.google.com/operations/"
                f"details/{operation_name.split('/')[-1]}?project={PROJECT_ID}"
            )
        }), 202

    except Exception as e:
        return jsonify({
            "error": "Failed to start batch process.",
            "details": str(e)
        }), 500


# -------------------------
# Cloud Run 启动
# -------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
