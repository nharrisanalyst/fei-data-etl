FROM python:3.9-slim-buster

WORKDIR /pipeline

COPY ./pipeline/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./pipeline .

EXPOSE 6060

CMD ["python", "pipeline.py"]