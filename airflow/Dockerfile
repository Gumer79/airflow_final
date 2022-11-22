FROM apache/airflow:2.4.2
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
#run fast_api server
CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "80"]


