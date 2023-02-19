from confluent_kafka import Consumer
import socket
import os
import argparse

class Consume :
    
    consumer = None
    msgCount = 0
    nthMsg = None

    def __init__(self, topic, print_nth_msg):
        self.name = "Consume"
        self.topic = topic
        self.nthMsg = print_nth_msg

    def intializeKafka(self) :
        bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")

        if bootstrap_servers is None:
            print("BOOTSTRAP_SERVERS environment variable not set. Defaulting to localhost:9092.")
            bootstrap_servers = "localhost:9092"
        
        print("Bootstrap servers: " + bootstrap_servers)

        
        conf = {'bootstrap.servers': bootstrap_servers,
        'group.id': 'consumer-1',
        'client.id': socket.gethostname(),
        'auto.offset.reset': 'earliest'}

        self.consumer = Consumer(conf)

        # Subscribe to topic
        self.consumer.subscribe([self.topic])


    def run(self):
        print ("Listening for events on topic: " + self.topic  + ". Press Ctrl-C to exit.")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                self.msgCount += 1
                if (self.msgCount < 2 or self.msgCount % int(self.nthMsg) == 0) :
                    print('Received message: {}'.format(msg.value().decode('utf-8')))
                
        except KeyboardInterrupt:
            pass
        

def main():
    topic = None
    nthMsg = None
    parser = argparse.ArgumentParser(description='Consume events from Kafka.')
    parser.add_argument('-t', '--topic', help='Topic to consume from', required=True)
    parser.add_argument('-p', '--print_nth_msg', help='When running in a loop, print every nth msg.', required=False)

    
    args = parser.parse_args()
    topic = args.topic
    nthMsg = 1 if args.print_nth_msg is None else args.print_nth_msg
    
    c = Consume(topic, nthMsg)
    
    c.intializeKafka()

    input("We are ready, press the enter key to continue")

    c.run()

    c.consumer.close()
    
    input("Press the enter key to exit the consume application.")

    

if __name__ == "__main__":
    main()