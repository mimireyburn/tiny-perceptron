FROM python:3.11
 
WORKDIR /src

COPY . .

RUN pip3 install -r requirements.txt

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]