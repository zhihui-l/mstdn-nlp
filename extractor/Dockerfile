FROM python:3.8
USER root
RUN pip install requests
WORKDIR /opt/app
COPY ./extractor.py .
CMD ["python", "./extractor.py"]