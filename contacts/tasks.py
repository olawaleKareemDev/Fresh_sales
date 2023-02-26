from celery import shared_task
from time import sleep
from django.core.mail import send_mail
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from freshsales_automations.celery import app




def send_email_task_now():
    message = Mail(
    from_email='olawalekareemdev@gmail.com',
    to_emails='markkareem100@gmail.com',
    subject='Sending with Twilio SendGrid is Fun',
    html_content='<strong>and easy to do anywhere, even with Python</strong>')
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e.message)



@shared_task
def sleepy(duration):
    sleep(duration)
    return None


@shared_task
def send_email_task():
    send_mail(
        'Celery task worked!',
        'The proof that the task worked',
        'olawalekareemdev@gmail.com',
        ['markkareem100@gmail.com']
    )
    return None


from django.urls import reverse



@app.task
def task_service_update_cooperate_data():
    print(" running task update cooperate data")
    reverse('freshsales-load-contacts-cooperate')
    # return "success"

@app.task
def task_freshsales_update_cooperate_data():
    print(" task one called and worker is running good")
    return "success"


@app.task
def task_service_update_individual_data():
    print(" task one called and worker is running good")
    return "success"

@app.task
def task_freshsales_update_individual_data():
    print(" task one called and worker is running good")
    return "success"


