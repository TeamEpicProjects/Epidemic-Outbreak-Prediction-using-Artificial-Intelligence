# specify base image
FROM continuumio/anaconda3:latest


WORKDIR /home

# add python file
COPY ZikaVirus .

RUN pip install -r requirements.txt

# specify the entery cmd
CMD ["python","app.py"]


