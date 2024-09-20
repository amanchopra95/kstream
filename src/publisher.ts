import { DeliveryReport, LibrdKafkaError } from "node-rdkafka";
import { Config } from "./config";
import { KStream } from "./kafka";

export class KafkaPublisher {
    constructor() {
        Config.init();
    }


    public async init() {

        const kStream = new KStream();

        await kStream.connect(this.deliveryReport);

        await kStream.produceMessages("Some message");

    }

    private deliveryReport(err: LibrdKafkaError, report: DeliveryReport) {
        if (err) {
            console.log(`Error on producing event`);
            console.error(err);
        } else {
            const {topic, key, value} = report;
            let k = key?.toString().padEnd(10, ' ');
            console.log(`Produced an event on topic: ${topic} key: ${k} value: ${value?.toString()}`);
        }
    }
}

const kafkaPublisher = new KafkaPublisher();
kafkaPublisher.init();