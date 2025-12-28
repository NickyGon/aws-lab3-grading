import json
import os
from io import BytesIO
from typing import Any, Dict, Optional
from urllib.parse import unquote_plus

import boto3
from PIL import Image, ExifTags

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

ALLOWED_EXTS = (".jpg", ".jpeg", ".png")


def _env(name: str, default: Optional[str] = None) -> str:
    val = os.environ.get(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _is_allowed_image_key(key: str) -> bool:
    return key.lower().endswith(ALLOWED_EXTS)


def _build_metadata_key(input_key: str, input_prefix: str, output_prefix: str) -> str:
    """
    Convert incoming/<path>/file.jpg -> metadata/<path>/file.jpg.json
    """
    rel = input_key[len(input_prefix):] if input_key.startswith(input_prefix) else input_key
    return f"{output_prefix}{rel}.json"


def ingest_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    S3 -> SQS (ingestion only)
    - Only accept objects under incoming/
    - Validate extension is .jpg/.jpeg/.png
    - Send message to SQS containing bucket, key, etag
    """
    queue_url = _env("QUEUE_URL")
    input_prefix = os.environ.get("INPUT_PREFIX", "incoming/")

    records = event.get("Records", []) or []
    sent = 0
    skipped = 0

    for record in records:
        try:
            bucket = record["s3"]["bucket"]["name"]
            key = unquote_plus(record["s3"]["object"]["key"])
            etag = record["s3"]["object"].get("eTag") or record["s3"]["object"].get("etag")
            print("INGEST EVENT:", {"bucket": bucket, "key": key, "etag": etag})
        except Exception:
            skipped += 1
            continue

        if not key.startswith(input_prefix):
            print("SKIP: wrong prefix", key)
            skipped += 1
            continue

        if not _is_allowed_image_key(key):
            print("SKIP: not allowed extension", key)
            skipped += 1
            continue

        body = {"bucket": bucket, "key": key, "etag": etag}
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))
        print("SENT TO SQS:", body)
        sent += 1

    return {"statusCode": 200, "sent": sent, "skipped": skipped}


def _safe_exif_dict(img: Image.Image) -> Dict[str, Any]:
    """
    Return EXIF data if present in a JSON-serializable dict.
    Keys are tag names when available; values are coerced to strings.
    """
    exif_out: Dict[str, Any] = {}
    try:
        exif = img.getexif()
        if not exif:
            return exif_out

        for tag_id, value in dict(exif).items():
            tag_name = ExifTags.TAGS.get(tag_id, str(tag_id))
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("utf-8", errors="replace")
            elif isinstance(value, (tuple, list)):
                value = [str(v) for v in value]
            else:
                value = str(value)
            exif_out[tag_name] = value
    except Exception:
        return {}
    return exif_out


def metadata_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    SQS -> S3 (processing worker)
    For each message:
      - download the image from S3
      - extract metadata (format, width, height, file size, exif if present)
      - write JSON to metadata/ prefix in the same bucket
    Idempotent: if metadata file already exists, skip.
    """
    output_prefix = os.environ.get("OUTPUT_PREFIX", "metadata/")
    input_prefix = os.environ.get("INPUT_PREFIX", "incoming/")

    processed = 0
    skipped = 0
    errors = 0

    print("SQS Records:", len(event.get("Records", []) or []))

    for record in event.get("Records", []) or []:
        try:
            body = json.loads(record.get("body", "{}"))
            bucket = body["bucket"]
            key = body["key"]
            etag = body.get("etag")
        except Exception as e:
            print("ERROR getting S3 object:", repr(e))
            errors += 1
            continue

        if not key.startswith(input_prefix) or not _is_allowed_image_key(key):
            skipped += 1
            continue

        out_key = _build_metadata_key(key, input_prefix=input_prefix, output_prefix=output_prefix)

        print("MSG:", body)
        print("OUT_KEY:", out_key)

        # Idempotency: if output exists, do not reprocess
        print("CHECK EXISTS:", {"bucket": bucket, "out_key": out_key})
        try:
            s3.head_object(Bucket=bucket, Key=out_key)
            print("SKIP: output already exists:", out_key)
            skipped += 1
            continue
        except s3.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            print("HEAD missing (expected):", code)
            if code not in ("404", "NoSuchKey", "NotFound"):
                errors += 1
                continue

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            img_bytes = obj["Body"].read()
            content_len = obj.get("ContentLength", len(img_bytes))
        except Exception as e:
            print("ERROR getting S3 object:", repr(e))
            errors += 1
            continue

        try:
            img = Image.open(BytesIO(img_bytes))
            width, height = img.size
            fmt = (img.format or "").upper()
            exif_data = _safe_exif_dict(img)
        except Exception as e:
            print("ERROR getting S3 object:", repr(e))
            errors += 1
            continue

        metadata = {
            "source_bucket": bucket,
            "source_key": key,
            "etag": etag,
            "format": fmt,
            "width": width,
            "height": height,
            "file_size_bytes": int(content_len),
            "exif": exif_data,
        }

        try:
            s3.put_object(
                Bucket=bucket,
                Key=out_key,
                Body=json.dumps(metadata, indent=2, sort_keys=True).encode("utf-8"),
                ContentType="application/json",
            )
            print("WROTE:", out_key)
            processed += 1
        except Exception as e:
            print("ERROR getting S3 object:", repr(e))
            errors += 1
            continue

    return {"statusCode": 200, "processed": processed, "skipped": skipped, "errors": errors}
