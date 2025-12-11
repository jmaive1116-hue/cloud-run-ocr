import os
import json
from flask import Flask, request, jsonify
from google.cloud import storage, documentai_v1 as documentai

app = Flask(__name__)

# -----------------------------------------------
# 文本块提取函数
# -----------------------------------------------
def extract_text_blocks(doc):
    """从 Document AI 返回结果中提取文本块，保留结构化信息"""
    text = doc.text
    blocks_output = []

    for page in doc.pages:
        # 1. blocks
        for block in page.blocks:
            block_text = ""
            for segment in block.layout.text_anchor.text_segments:
                start = segment.start_index or 0
                end = segment.end_index
                block_text += text[start:end]

            blocks_output.append({
                "type": "block",
                "page": page.page_number,
                "text": block_text.strip()
            })

        # 2. paragraphs
        for para in page.paragraphs:
            para_text = ""
            for segment in para.layout.text_anchor.text_segments:
                start = segment.start_index or 0
                end = segment.end_index
                para_text += text[start:end]

            blocks_output.append({
                "type": "paragraph",
                "page": page.page_number,
                "text": para_text.strip()
            })

    return blocks_output

# -----------------------------------------------
# 调用 Document AI OCR
# -----------------------------------------------
def process_document_ai(project_id, location, processor_id, gcs_input_uri):
    client = documentai.DocumentProcessorServiceClient()
    name = client.processor_path(project_id, location, processor_id)

    request = documentai.ProcessRequest(
        name=name,
        skip_human_review=True,
        input_documents=documentai.BatchDocumentsInputConfig(
            gcs_documents=documentai.GcsDocuments(documents=[
                documentai.GcsDocument(
                    gcs_uri=gcs_input_uri,
                    mime_type="application/pdf"
                )
            ])
        )
    )

    result = client.process_document(request=request)
    blocks = extract_text_blocks(result.document)
    return blocks

# -----------------------------------------------
# 列出 GCS bucket 下所有 PDF
# -----------------------------------------------
def list_pdfs_in_gcs(bucket_name, prefix=""):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    pdf_files = [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.lower().endswith(".pdf")
    ]

    return pdf_files

# -----------------------------------------------
# 保存 JSON 到 GCS
# -----------------------------------------------
def save_blocks_to_gcs(bucket_name, output_path, blocks):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_path)

    blob.upload_from_string(
        json.dumps(blocks, ensure_ascii=False, indent=2),
        content_type="application/json"
    )

# -----------------------------------------------
# 批处理函数
# -----------------------------------------------
def run_batch_ocr():
    # 必须使用环境变量名，而不是项目名/桶名
    PROJECT_ID = os.environ["PROJECT_ID"]
    LOCATION = os.environ["PROCESSOR_LOCATION"]
    PROCESSOR_ID = os.environ["PROCESSOR_ID"]
    INPUT_BUCKET = os.environ["INPUT_BUCKET"]
    OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
    INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "")

    pdf_files = list_pdfs_in_gcs(INPUT_BUCKET, INPUT_PREFIX)
    results = []

    for gcs_uri in pdf_files:
        print(f"Processing {gcs_uri}")
        try:
            blocks = process_document_ai(PROJECT_ID, LOCATION, PROCESSOR_ID, gcs_uri)
            filename = gcs_uri.split("/")[-1].replace(".pdf", ".json")
            output_path = f"ocr_results/{filename}"
            save_blocks_to_gcs(OUTPUT_BUCKET, output_path, blocks)
            results.append({
                "input": gcs_uri,
                "output": f"gs://{OUTPUT_BUCKET}/{output_path}",
                "status": "success"
            })
        except Exception as e:
            print(f"Error processing {gcs_uri}: {e}")
            results.append({
                "input": gcs_uri,
                "error": str(e),
                "status": "failed"
            })

    return results
# -----------------------------------------------
# HTTP 端点
# -----------------------------------------------
@app.route("/", methods=["GET"])
def health_check():
    return "Batch OCR service is running.", 200

@app.route("/run-batch", methods=["GET", "POST"])
def run_batch():
    results = run_batch_ocr()
    return jsonify({"processed": results})

# -----------------------------------------------
# Cloud Run 启动入口
# -----------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


