FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WQ_DATA_DIR=/data \
    WQ_DOWNSTREAM_DIR=/downstream

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY wsgi.py ./
COPY wq_publish ./wq_publish

RUN mkdir -p /data /downstream

EXPOSE 5000

# healthcheck 用 python 标准库发请求，镜像无需额外工具。
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5000/', timeout=3)"

# 单进程多线程即可：并发正确性依赖 SQLite 的 BEGIN IMMEDIATE 与同目录原子改名。
CMD ["waitress-serve", "--call", "--host=0.0.0.0", "--port=5000", "--threads=8", "wsgi:create_app"]
