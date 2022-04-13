import * as enginemq from '../../index';

const args = process.argv.slice(2);

enginemq.defaultEngineMqPublishClientOptions.timeoutMs = 100;

const client = new enginemq.EngineMqClient({
    clientId: 'example-resend',
    // authToken: '????', 
    connectAutoStart: false,
    maxWorkers: 4
});

client.on('mq-connected', (reconnectCount) => console.log("Connected: " + reconnectCount));
client.on('mq-error', (errorCode: string, errorMessage: string, data: any) => console.log("Error " + errorCode + ': ' + errorMessage, data));
client.on('mq-disconnected', () => console.log("Disconnected"));

client.on('mq-ready', () => {
    console.log("Ready");

    client.subscribe(['log.event.*', 'log.#.plugins']);
    setInterval(async () => {
        await publish(20);
    }, 30 * 1000);
});

client.on('mq-message', (
    ack: enginemq.EngineMqPublishDeliveryAck,
    topic: string,
    data: object,
    delivery: enginemq.types.BrokerMessageDelivery
) => {
    console.dir(`Received message from ${topic} (id=${delivery.options.messageId}): ${JSON.stringify(data)}`);

    if (Math.random() < 0.2)
        ack.reject({ reason: "DB error" });
    else {
        ack.start();
        setTimeout(() => ack.progress(25), 200);
        setTimeout(() => ack.progress(50), 400);
        setTimeout(() => ack.progress(75), 600);
        setTimeout(() => ack.finish(), 800);
    }
});

client.on('mq-delivery-report', (report: enginemq.EngineMqMessageDeliveryReport) => {
    console.log(report);
});

client.connect();

const publish = async (count: number): Promise<void> => {
    try {
        if (client.connected) {
            for (let i = 0; i < count; i++)
                await client.publish(
                    'log.wordpress.plugins',
                    {
                        mimeType: 'application/string',
                        str: `Example data #${i}`,
                    });
            console.log(`Published ${count} messages`);
        }
    }
    catch (error) {
        console.log(`Error: ${error instanceof Error ? error.message : ''}`)
    }
}
