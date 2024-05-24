"""

    RabbitMQ Project - Version 3
    Automating Tasks from a CSV file to RabbitMQ.

    Author: Julia Fangman
    Date: May 23, 2024

"""

# Imports
import pika
import sys
import webbrowser
import csv

# Configure Logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# Offer to open RabbitMQ Admin Page
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info("Opened RabbitMQ")

def send_message(host: str, queue_name: str, message: str, username: str, password: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
        username (str): the username for RabbitMQ authentication
        password (str): the password for RabbitMQ authentication
    """

    conn = None
    try:
        # Set up credentials
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(host, 5672, '/', credentials)
        
        # Create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(parameters)
        
        # Use the connection to create a communication channel
        ch = conn.channel()
        
        # Use the channel to declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)
        
        # Use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        
        # Print a message to the console for the user
        logger.info(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # Close the connection to the server if it was opened
        if conn and conn.is_open:
            conn.close()

# Read tasks from CSV and send to RabbitMQ server
def read_and_send_tasks_from_csv(file_path: str, host: str, queue_name: str, username: str, password: str):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader: 
            message = " ".join(row)
            send_message(host, queue_name, message, username, password)

if __name__ == "__main__":
    # Offer to open RabbitMQ admin page
    offer_rabbitmq_admin_site()
    
    # Define file_name variables file_name, host, and queue_name 
    file_name = 'tasks.csv'
    host = "localhost"
    queue_name = "task_queue3"
    
    # Define RabbitMQ credentials
    username = 'julia_fangman'
    password = 'Banana789!'
    
    # Send the tasks to the queue
    read_and_send_tasks_from_csv(file_name, host, queue_name, username, password)
