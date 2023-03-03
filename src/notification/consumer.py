import os
import sys
import time

import pika
from send import email


def main():
    """
    Connects to RabbitMQ and waits for messages on the MP3_QUEUE.
    When a message is received, it calls the new_func function to process it.
    If the function returns an error, it sends a negative acknowledgement (nack) to RabbitMQ.
    If the function returns successfully, it sends a positive acknowledgement (ack) to RabbitMQ.
    """
    # rabbitmq connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()

    def callback(ch, method, body):
        """
        Callback function that is called when a message is received.
        Calls the new_func function to process the message.
        Sends an ack or nack to RabbitMQ depending on whether the function returns an error or not.
        """
        err = new_func(body)
        if err:
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def new_func(body):
        """
        Function that processes the message body.
        Calls the email.notification function to send an email notification.
        Returns an error message if the email sending fails, otherwise returns None.
        """
        err = email.notification(body)
        return err

    channel.basic_consume(
        queue=os.environ.get("MP3_QUEUE"), on_message_callback=callback
    )

    print("Waiting for messages. To exit press CTRL+C")

    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
