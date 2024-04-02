FROM python:3.11

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./source /app/source

ENV PYTHONPATH=/app

CMD ["uvicorn", "source.app:api", "--host", "0.0.0.0", "--port", "80"]