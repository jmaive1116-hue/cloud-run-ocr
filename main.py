from google.cloud import storage, documentai_v1 as documentai
from flask import Flask

app = Flask(__name__)

@app.route("/")
def run_ocr_batch():
    project_id = "GeoTech-Research-Assistant"
    location = "us"   # 通常用 us
    processor_id = "Geotech_Paper_OCR_Processor"
    bucket_name = "geotech-papers-jinghu"
    input_prefix = "papers/"      # 要处理的 PDF 存放目录
    output_prefix = "ocr_output/" # OCR 输出目录

    docai_client = documentai.DocumentProcessorServiceClient()
    storage_client = storage.Client()

    name = docai_client.processor_path(project_id, location, processor_id)
    bucket = storage_client.bucket(bucket_name)

    # 自动列出所有 PDF 文件
    pdf_files = [
        documentai.GcsDocument(
            gcs_uri=f"gs://{bucket_name}/{blob.name}",
            mime_type="application/pdf"
        )
        for blob in bucket.list_blobs(prefix=input_prefix)
        if blob.name.endswith(".pdf")
    ]

    if not pdf_files:
        return "No PDF found."

    input_docs = documentai.GcsDocuments(documents=pdf_files)
    input_config = documentai.BatchDocumentsInputConfig(gcs_documents=input_docs)

    output_config = documentai.DocumentOutputConfig(
        gcs_output_config=documentai.DocumentOutputConfig.GcsOutputConfig(
            gcs_uri=f"gs://{bucket_name}/{output_prefix}"
        )
    )

    request = documentai.BatchProcessRequest(
        name=name,
        input_documents=input_config,
        document_output_config=output_config,
    )

    op = docai_client.batch_process_documents(request)
    op.result()

    return f"OCR completed. Output in gs://{bucket_name}/{output_prefix}"
