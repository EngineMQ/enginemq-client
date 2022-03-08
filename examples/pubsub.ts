import * as enginemq from '../index';

const args = process.argv.slice(2);

enginemq.defaultEngineMqPublishClientOptions.timeoutMs = 100;

const client = new enginemq.EngineMqClient({ clientId: 'xmpl-pubsub', connectAutoStart: false, maxWorkers: 4 });

client.on('mq-connected', (reconnectCount) => console.log("Connected: " + reconnectCount));
client.on('mq-ready', (serverVersion, heartbeatSec) => {
    console.log(["Ready: ", serverVersion, heartbeatSec]);
    setTimeout(async () => {
        if (args[0] != 'nopub')
            await publish(50, 100);
        client.subscribe(['log.event.#', 'log.*.wordpress']);
    }, 1000);
});
client.on('mq-no-heartbeat', () => console.log("Missing heartbeat"));
client.on('mq-disconnected', () => console.log("Disconnected"));

client.on('mq-message', (
    ack: enginemq.EngineMqPublishDeliveryAck,
    topic: string,
    data: object,
    delivery: enginemq.types.BrokerMessageDelivery
) => {
    console.dir(`[${topic}]@${delivery.options.messageId} ${JSON.stringify(data)}`);
    ack.start();
    setTimeout(() => { ack.progress(25) }, 250);
    setTimeout(() => { ack.progress(50) }, 500);
    setTimeout(() => { ack.progress(75) }, 750);
    setTimeout(() => { ack.finish() }, 1000);
    ack.resolve();
});

// client.subscribe(['log.event.#', 'log.*.wordpress']);
client.connect();

const publish = async (count: number, reinit: number = 0): Promise<void> => {
    try {
        if (client.connected) {
            for (let i = 0; i < count; i++)
                await client.publish(
                    'log.analitics.wordpress',
                    { str: `Example data #${i}` },
                    {
                        priority: i % 2 == 0 ? enginemq.types.MessagePriority.High : enginemq.types.MessagePriority.Normal,
                        //delayMs: 15000,
                        //expirationMs: i % 4 == 0 ? 1400 : 0,
                    });
            console.log("Published all");
        }
    }
    catch (error) {
        console.log(`Error: ${error instanceof Error ? error.message : ''}`)
    }
    if (reinit)
        setTimeout(publish, reinit, count, reinit);
}
