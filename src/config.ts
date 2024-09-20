import config from "./config.json";

export class Config {
    public static bootStrapServers: string | undefined;
    public static topic: string | undefined;
    public static clientId: string | undefined;
    public static deliveryReportEnabled: boolean | undefined;

    public static init() {
        Config.bootStrapServers = config["bootstrap.servers"];
        Config.clientId = config["client.id"];
        Config.topic = config.topic;
        Config.deliveryReportEnabled = config.dr_cb;
    }
}