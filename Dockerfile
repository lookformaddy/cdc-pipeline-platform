FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir -e .
EXPOSE 8000
CMD ["python", "-m", "cdc_pipeline", "serve"]
