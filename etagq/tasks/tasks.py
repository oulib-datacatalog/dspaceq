from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests

#Default base directory 
basedir="/data/static/"


#Example task
@task()
def add(x, y):
    result = x + y
    return result
