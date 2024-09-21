import { Config } from "./config";
import { KStream } from "./kafka";

export class KafkaConsumer {
    constructor() {
        Config.init();
    }

    public async init() {
        const kStream = new KStream();
        await kStream.consumerConnect(this.onData);
        kStream.consumeMessages();
    }

    private onData(data: any) {
        let {key, value} = data;
        let k;
        if (key) {
            k = key.toString().padEnd(10, ' ');
        }
        console.log(`Consumed event from topic ${Config.topic!} key: ${k} value: ${value}`);
    }
}

const kConsumer = new KafkaConsumer();
kConsumer.init();