import * as enginemq from '../../index';
import { largeText } from './ex-source';

const args = process.argv.slice(2);

enginemq.defaultEngineMqPublishClientOptions.timeoutMs = 100;

const client = new enginemq.EngineMqClient({
    clientId: 'example-autopubsub',
    // authToken: '????', 
    connectAutoStart: false,
    maxWorkers: 4
});

client.on('mq-connected', (reconnectCount) => console.log("Connected: " + reconnectCount));
client.on('mq-error', (errorCode: string, errorMessage: string, data: any) => console.log("Error " + errorCode + ': ' + errorMessage, data));
client.on('mq-disconnected', () => console.log("Disconnected"));

client.on('mq-ready', async () => {
    console.log("Ready");

    client.subscribe(['log.event.*', 'log.#.plugins']);

    await publish(20);
    setInterval(async () => {
        await publish(20);
    }, 3 * 1000);
});

client.on('mq-message', (
    ack: enginemq.EngineMqPublishDeliveryAck,
    topic: string,
    data: any,
    delivery: enginemq.types.BrokerMessageDelivery
) => {
    if (data.mimeType == 'application/filedata')
        if (data.content.length > 100)
            data.content = `size: ${data.content.length}`;

    console.dir(`Received message from ${topic} (id=${delivery.options.messageId}): ${JSON.stringify(data)}`);

    ack.finish();
});

client.connect();

const publish = async (count: number): Promise<void> => {
    try {
        if (client.connected) {
            for (let i = 0; i < count; i++) {
                let msg: object = {};
                switch (Math.floor(Math.random() * 3)) {
                    case 0:
                        msg = {
                            mimeType: 'application/string',
                            str: `Example data #${i}`,
                        };
                        break;
                    case 1:
                        msg = {
                            mimeType: 'application/object',
                            customer: {
                                firstName: 'Adam',
                                lastName: 'Family',
                                addresses: [
                                    {
                                        zip: 'H-1234',
                                        city: 'Houston',
                                        address: 'Main str. 12.',
                                    },
                                    {
                                        zip: 'GR-8887',
                                        city: 'Denver',
                                        address: 'Substream str. 0.',
                                    },
                                ],
                            },
                        };
                        break;
                    case 2:
                        msg = {
                            mimeType: 'application/filedata',
                            content: largeText,
                        };
                        break;
                }
                await client.publish(
                    'log.wordpress.plugins',
                    msg);
            }
            console.log(`Published ${count} messages`);
        }
    }
    catch (error) {
        console.log(`Error: ${error instanceof Error ? error.message : ''}`)
    }
}
