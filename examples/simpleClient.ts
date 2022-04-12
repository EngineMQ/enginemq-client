import * as enginemq from '../index';
//import * as enginemq from 'enginemq-client';

const args = process.argv.slice(2);

enginemq.defaultEngineMqPublishClientOptions.timeoutMs = 1500;

const client = new enginemq.EngineMqClient({ clientId: 'Cli-ONE', authToken: 'iF3R0Hn6XKrwUbFaB7shot9uUratjOVI', connectAutoStart: false, maxWorkers: 4 });

client.on('mq-connected', (reconnectCount) => console.log("Connected: " + reconnectCount));
client.on('mq-ready', (serverVersion, heartbeatSec) => {
    console.log(["Ready: ", serverVersion, heartbeatSec]);
    if (args[0] == 'publish')
        loop();
});
client.on('mq-no-heartbeat', () => console.log("Missing heartbeat"));
client.on('mq-disconnected', () => {
    breakLoop = true;
    console.log("Disconnected")
});

let max = Number.MIN_SAFE_INTEGER;
let min = Number.MAX_SAFE_INTEGER
let start = (new Date()).getTime();
let cnt = 0;
//let parallel = 0;
client.on('mq-message', (
    ack: enginemq.EngineMqPublishDeliveryAck,
    topic: string,
    data: object,
    messageInfo: enginemq.types.BrokerMessageDelivery) => {
    //console.dir(`[${topic}] ${JSON.stringify(data)} ${delivery.options.messageId}`);
    topic;
    data;
    messageInfo;
    //console.log(data)
    ack;
    //setTimeout(ack, 0);

    cnt++;
    if (cnt % 100 == 0) {
        const time = (new Date()).getTime() - start;

        if (time) {
            const speed = Math.round(cnt / time * 1000);
            if (speed > max) max = speed;
            if (speed < min) min = speed;
            console.log([min, max, speed]);
        }
        cnt = 0;
        start = (new Date()).getTime();
    }
    ack.finish();
});

if (args[0] == 'subscribe')
    client.subscribe(['log.event.#', 'log.*.wordpress', 'log.*']);
client.connect();
console.log(client.subscriptions);

// setTimeout(() => {
//     client.unsubscribe('a.b.c');
//     console.log(client.subscriptions);
// }, 1500);

//function delay(ms: number) { return new Promise(resolve => setTimeout(resolve, ms)); }

let breakLoop = false;
const loop = async () => {
    let mode = 0;
    let count = 0;
    breakLoop = false;
    while (!breakLoop) {
        try {
            switch (mode) {
                case 0:
                    //console.log('ST' + (new Date()).getTime())
                    /*const msgId =*/
                    await client.publish(
                        'log.analitics.wordpress',
                        { str: 'Example string' + (new Date()).getTime() },
                        {
                            priority: Math.floor(Math.random() * (enginemq.types.MAX_MESSAGE_PRIORITY - 1))
                        },
                        { timeoutMs: 1500 })
                    break;
                case 1:
                    await client.publish(
                        'log.event.wordpress',
                        {
                            data: [
                                'Example array',
                                'Array item2',
                                'Az Index megkereste a pénzintézetet, dolgoznak a probléma megoldásán.',
                                'A közelmúltban publikált véleményszonda eredménye nem kevés tanulsággal szolgál.', 'Az ügyfelek bankkártyás tranzakciókat igen, de számlaműveleteket nem tudnak indítani.',
                                'Tagdíjbevételük is gyarapodott: meghaladja a 140 milliárd forintot. A pénztárak jóval kisebb eredményt értek el tavaly, mint a megelőző évben. Portfóliójukban nőtt a bankszámlákon tartott eszközök aránya és a részvényeké, de csökkent a kötvényeké.',
                                'Az egészség- és önsegélyező pénztárak tagjainak vagyona mintegy négy százalékkal, több mint 70 milliárd forintra emelkedett. Tovább apadt a magánnyugdíjpénztárak taglétszáma, mindössze ötvenezren voltak tavaly.',
                                'Több mint hat százalékkal, mintegy 1729 milliárd forintra nőtt tavaly a magyarországi önkéntes nyugdíjpénztárak vagyona - derül ki a Magyar Nemzeti Bank adataiból. A nyugdíjpénztáraknak több mint egymillió tagjuk van - írja a vg.hu.',
                            ]
                        },
                        {
                            qos: enginemq.types.MessageQos.Normal,
                            priority: enginemq.types.MessagePriority.Normal
                        })
                    break;
                case 2:
                    await client.publish(
                        'log.event.erp',
                        { title: 'Example string', number: 12, memo: '' },
                        {
                            qos: enginemq.types.MessageQos.Normal,
                            priority: Math.floor(Math.random() * (enginemq.types.MAX_MESSAGE_PRIORITY + 1))
                        })
                    break;
            }
        }
        catch (error) { if (error instanceof Error) console.log("HIBA: " + error.message) };
        if (++mode > 2) mode = 0;
        count++;
        if (count % 1000 == 0) {
            console.log(count);
            //await delay(100);
        }
    };
}

//loop();