FROM python:3.11

RUN apt-get update -y

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["python"]

CMD ["AsincBot.py"]
