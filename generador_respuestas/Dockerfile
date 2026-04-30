FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .
COPY Z1.csv Z2.csv Z3.csv Z4.csv Z5.csv .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
