# microkafka
A bare-bones implementation of the Kafka protocol. My goal is to learn about some of the inner workings of Kafka. 

In its current state, the code can simulate topic creation. It spins up the server (TCP listener), when we run run `kafka-topics.sh --create`, the server communicates in Kafka's protocol to make the CLI client think the topic was created. The code aims to achieve this in the simplest way. See the `TODO` section for future plans.


# Testing
I am running `go1.23.4` for my local testing.

1. Start the server locally on port 9092:

    ```go
    go run main.go
    ```
2. I am testing with Kafka 3.9:

    ```bash
    bin/kafka-topics.sh --create --topic mytopic  --bootstrap-server localhost:9092    

    Created topic mytopic.
    ```


# TODO
- [X] Simulate Topic Creation
- [ ] Actually parse requests (so far, only topic name is retrieved)
- [ ] Produce
- [ ] Fetch

# Credits
[Jocko](https://github.com/travisjeffery/jocko) has been a major inspiration for this work.