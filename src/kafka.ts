import { Client, ClientMetrics, KafkaConsumer, Producer, ProducerGlobalConfig, SubscribeTopicList } from "node-rdkafka";
import { Config } from "./config";

export class KStream {
    private _producer: Producer;
    private _consumer: KafkaConsumer;

    constructor() {
        this._producer = new Producer({
            "metadata.broker.list": Config.bootStrapServers,
            "dr_cb": Config.deliveryReportEnabled,
            "client.id": Config.clientId
        });
        this._consumer = new KafkaConsumer({
            "metadata.broker.list": Config.bootStrapServers,
            "group.id": Config.groupId

        }, {
            "auto.offset.reset": 'earliest'
        });
    }


    public async producerConnect(deliveryReportCb: any) {
        if (!this._producer) {
            throw new Error("Producer not initialized");
        }

        return new Promise((resolve, reject) => {
            this._producer
            .on('ready', () => {
                console.log(`Connected to Kafka Succesfully`);
                resolve(this._producer);
            })
            .on("delivery-report", deliveryReportCb)
            .on("connection.failure", (err, metrics) => {
                console.log(`Connection Failure metrics : ${JSON.stringify(metrics)}`);
                console.error(err);
                reject(err);
            })
            .on("event.error", (err) => {
                console.log(`Event Error`);
                console.error(err);
                reject(err);
            })
            .on("disconnected", () => {
                console.log(`Disconnected from Kafka`);
            })
            this._producer.connect();
        })
        
    }

    public async consumerConnect(cb: any) {
        if (!this._consumer) {
            throw new Error(`Consumer not initialized`);
        }

        return new Promise((resolve, reject) => {
            this._consumer
            .on("ready", () => {
                resolve(this._consumer);
            })
            .on("connection.failure", (err, metrics) => {
                console.log(`Connection Failure metrics : ${JSON.stringify(metrics)}`);
                console.error(err);
                reject(err);
            })
            .on("event.error", (err) => {
                console.log(`Event Error`);
                console.error(err);
                reject(err);
            })
            .on("disconnected", (metric: ClientMetrics) => {
                console.log(`Connection disconnected metrics: ${JSON.stringify(metric)}`);
            })
            .on("subscribed", (topics: SubscribeTopicList) => {
                console.log(`Subscribed to the following topics: ${JSON.stringify(topics)}`);
            })
            .on("unsubscribed", () => {
                console.log(`Unsubscribed to the topic`);
            })
            .on("data", cb);

            this._consumer.connect();
        })
    }

    public async produceMessages(data: string) {
        if (!this._producer.isConnected) {
            throw new Error(`Kafka not connected`);
        }
        
        const message = Buffer.from(data);
        console.log("Going to produce message on topic: " + Config.topic!);
        this._producer.produce(Config.topic!, -1, message);
    }

    public async consumeMessages() {
        if (!this._consumer.isConnected) {
            throw new Error(`Kafka consumer not connected`);
        }

        try {
            this._consumer.subscribe([Config.topic!]);
        } catch (err) {
            console.error(err);
        }
        this._consumer.consume();
    }
}