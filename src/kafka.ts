import { Producer, ProducerGlobalConfig } from "node-rdkafka";
import { Config } from "./config";

export class KStream {
    private _producer: Producer;

    constructor() {
        this._producer = new Producer({
            "metadata.broker.list": Config.bootStrapServers,
            "dr_cb": Config.deliveryReportEnabled,
            "client.id": Config.clientId
        });
    }


    public async connect(deliveryReportCb: any) {
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

    public async produceMessages(data: string) {
        if (!this._producer.isConnected) {
            throw new Error(`Kafka not connected`);
        }

        const message = Buffer.from(data);
        this._producer.produce(Config.topic!, null, message, "some-random-word");
    }
}