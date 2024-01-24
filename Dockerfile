FROM python:3.11-alpine
LABEL maintainer="tomasz.wolski@fujitsu.com"
COPY . /service
WORKDIR /service
RUN pip install -r requirements.txt
EXPOSE 8080
ENTRYPOINT ["python"]
CMD ["src/app.py"]
