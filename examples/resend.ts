import * as enginemq from '../index';

enginemq.defaultEngineMqPublishClientOptions.timeoutMs = 100;

const client = new enginemq.EngineMqClient({ clientId: 'xmpl-resend', connectAutoStart: false, maxWorkers: 4 });
let index = 0;

client.on('mq-ready', (serverVersion, heartbeatSec) => {
    console.log(["Ready: ", serverVersion, heartbeatSec]);
    setInterval(async () => {
        if (client.connected)
            try {
                for (let j = 0; j < 1; j++) {
                    index++;
                    await client.publish(
                        'log.analitics.wordpress',
                        { str: `Example data ${index}` },
                        {
                            messageId: 'fix00' + index,
                            qos: enginemq.MessageQos.Feedback,
                        });
                }
            }
            catch { }
    }, 1000);
});

client.on('mq-message', (
    ack: enginemq.EngineMqPublishDeliveryAck,
    topic: string,
    data: object,
    delivery: enginemq.types.BrokerMessageDelivery
) => {
    console.dir(`[${topic}]@${delivery.options.messageId} ${JSON.stringify(data)}`);
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

client.subscribe(['log.event.#', 'log.*.wordpress']);
client.connect();
