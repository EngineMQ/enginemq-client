import { version as libraryEngineMQVersion } from './package.json';
import { customAlphabet } from 'nanoid';
import * as net from 'node:net';

import * as types from './common/messageTypes';
import { validateObject } from './common/lib/ajv';
import { MsgpackSocket } from './common/lib/socket/MsgpackSocket';

export * as types from './common/messageTypes';

const HEARTBEAT_FREQ_PERCENT = 45;
const RECONNECT_MAX_WAIT = 750;
const ACK_WAITINGLIST_TIMEOUT_SEC = 10;
const ACK_WAITINGLIST_MIN_LENGTH = 100;

const nanoid = customAlphabet(types.MESSAGE_ID_ALPHABET, types.MESSAGE_ID_LENGTH_DEFAULT);

export class EngineMqClientError extends Error { }

type SendMessageFunction = (cm: types.ClientMessageType, object: object) => void;
type AckResolver = (errorMessage?: string) => void;
type PublishAckWaitingListItem = { resolver: AckResolver, waitingStartAt: number };

export declare interface EngineMqClient extends MsgpackSocket {
    on(event: 'connect', listener: () => void): this;
    on(event: 'data', listener: (data: Buffer) => void): this;
    on(event: 'buffered_data', listener: (data: Buffer) => void): this;
    on(event: 'end', listener: () => void): this;
    on(event: 'close', listener: (hadError: boolean) => void): this;
    on(event: 'obj_data', listener: (object: object) => void): this;

    on(event: 'mq-connected', listener: (reconnectCount: number) => void): this;
    on(event: 'mq-ready', listener: () => void): this;
    on(event: 'mq-disconnected', listener: () => void): this;
    on(event: 'mq-message', listener: (
        ack: EngineMqPublishDeliveryAck,
        topic: string,
        data: object,
        messageInfo: types.BrokerMessageDelivery
    ) => void): this;
    on(event: 'mq-delivery-report', listener: (report: types.BrokerMessageDeliveryReport) => void): this;
    on(event: 'mq-error', listener: (errorType: string, errorMessage: string, data?: any) => void): this;
}
export class EngineMqClient extends MsgpackSocket {
    private _allowReconnect = true;
    private _reconnectTimer = 0;
    private _connected = false;
    private _reConnected = 0 - 1;
    private _ready = false;
    private _params: Required<EngineMqClientParameters>;
    private _subscriptions: string[] = [];

    get ready() { return this._ready; }
    get connected() { return this._connected; }
    get reconnectCount() { return this._reConnected; }
    get reconnectAllowed() { return this._allowReconnect; }
    set reconnectAllowed(value: boolean) { this._allowReconnect = value; if (value) this.reConnect(0) }

    constructor(parameters: EngineMqClientParameters) {
        super(new net.Socket());

        this._params = { ...defaultEngineMqClientParameters, ...parameters };

        this.on('obj_data', (object: object) => this.onObjData(object));

        this.on('connect', () => {
            this._connected = true;
            this._reConnected++;
            this.lastRcvHeartbeat = Date.now();
            this.emit('mq-connected', this.reconnectCount);

            const cmLogin: types.ClientMessageLogin = {
                clientId: this._params.clientId,
                authToken: this._params.authToken,
                maxWorkers: this._params.maxWorkers,
                version: libraryEngineMQVersion,
            };
            cmLogin.clientId = (cmLogin.clientId || '').toLowerCase();
            this.sendMessage('login', cmLogin);
        });
        this.on('close', () => {
            this.clearHeartbeat();
            if (this._connected)
                this.emit('mq-disconnected');

            this._connected = false;
            this._ready = false;
            this.reConnect();
        });

        if (parameters.connectAutoStart)
            this.reConnect(0);
    }



    // Public

    public override connect() {
        this._allowReconnect = true;
        this.reConnect(0);
    }

    public close() {
        this._allowReconnect = false;
        if (this._reconnectTimer)
            clearTimeout(this._reconnectTimer);
        super.end();
    }

    public subscribe(channels: string | string[]) {
        if (!Array.isArray(channels))
            channels = [channels];
        for (const channel of channels) {
            if (!types.TOPIC_WILDCARD_MASK.test(channel))
                throw new EngineMqClientError(`EngineMQ subscribe invalid topic format: ${channel}`)
            if (channel.length > types.TOPIC_LENGTH_MAX)
                throw new EngineMqClientError(`EngineMQ subscribe topic too long: ${channel}`)
            if (!this._subscriptions.includes(channel))
                this._subscriptions.push(channel);
        }
        this.updateSubscriptions();
    }
    public unsubscribe(channels: string | string[]) {
        if (!Array.isArray(channels))
            channels = [channels];
        for (const channel of channels) {
            const index = this._subscriptions.indexOf(channel);
            if (index >= 0)
                this._subscriptions.splice(index, 1);
        }
        this.updateSubscriptions();
    }
    public unsubscribeAll() {
        this._subscriptions = [];
        this.updateSubscriptions();
    }
    public get subscriptions() { return this._subscriptions; }

    public async publish(topic: string,
        message: object,
        messageOptions: Partial<types.ClientMessagePublishOptions> = defaultEngineMqPublishMessageOptions,
        clientOptions: EngineMqPublishClientOptions = defaultEngineMqPublishClientOptions): Promise<string> {
        if (!this._connected) throw new EngineMqClientError('EngineMQ client not connected');
        if (!this._ready) throw new EngineMqClientError('EngineMQ client not in ready state');

        messageOptions = { ...defaultEngineMqPublishMessageOptions, ...messageOptions };
        clientOptions = { ...defaultEngineMqPublishClientOptions, ...clientOptions };
        if (!messageOptions.messageId)
            messageOptions.messageId = nanoid();

        if (!new RegExp(types.MESSAGE_ID_MASK).test(messageOptions.messageId))
            throw new EngineMqClientError(`EngineMQ publish invalid messageId format: ${messageOptions.messageId}`);
        if (!types.TOPIC_MASK.test(topic))
            throw new EngineMqClientError(`EngineMQ publish invalid topic format: ${topic}`);
        if (topic.length > types.TOPIC_LENGTH_MAX)
            throw new EngineMqClientError(`EngineMQ publish topic too long: ${topic}`);
        if (messageOptions.delayMs && messageOptions.delayMs < 0)
            throw new EngineMqClientError(`EngineMQ publish invalid delayMs value: ${messageOptions.delayMs}`);
        if (messageOptions.expirationMs && messageOptions.expirationMs < 0)
            throw new EngineMqClientError(`EngineMQ publish invalid expirationMs value: ${messageOptions.expirationMs}`);

        const cmPublish: types.ClientMessagePublish = { topic, message, options: messageOptions as types.ClientMessagePublishOptions };
        cmPublish.topic = (cmPublish.topic || '').toLowerCase();
        cmPublish.options.messageId = cmPublish.options.messageId.toLowerCase();
        this.sendMessage('publish', cmPublish);

        return new Promise((resolve, reject) => {
            const timerTimeout = setTimeout(reject, clientOptions.timeoutMs, new EngineMqClientError('Publish timeout'));
            const messageId = (messageOptions as types.ClientMessagePublishOptions).messageId;
            this.addToPublishAckWaitingList(
                messageId,
                (errorMessage?: string) => {
                    clearTimeout(timerTimeout);
                    if (errorMessage)
                        reject(new Error(errorMessage))
                    else
                        resolve(messageId);
                });
        });
    }



    // Private

    private reConnect(intervalMs = RECONNECT_MAX_WAIT) {
        if (this._allowReconnect && !this._connected)
            this._reconnectTimer = setTimeout(
                () => super.connect(this._params.port, this._params.host),
                Math.round(Math.random() * intervalMs)) as unknown as number;
    }

    private sendMessage: SendMessageFunction = (cm: types.ClientMessageType, object: object) => {
        const data: { [name: string]: object } = {};
        data[cm as keyof object] = object;
        super.sendObj(data);
        this.lastSndHeartbeat = Date.now();
    }

    private lastRcvHeartbeat: number = Date.now();
    private lastSndHeartbeat = 0;
    private processHeartbeat(sec: number) {
        const now = Date.now();
        if (now - this.lastSndHeartbeat > sec * 1000 / 100 * HEARTBEAT_FREQ_PERCENT) {
            const cmHeatbeat: types.ClientMessageHeartbeat = {};
            this.sendMessage('heartbeat', cmHeatbeat);
        }
        if (now - this.lastRcvHeartbeat > sec * 1000) {
            this.clearHeartbeat();
            this.emit('mq-error', 'HEARTBEAT', 'Heartbeat did not arrive on time');
            this.destroy();
        }
    }
    private timerHeartbeat = 0;
    private initHeartbeat(sec: number) {
        if (!sec) return;
        this.timerHeartbeat = setInterval(() => this.processHeartbeat(sec), sec * 100) as unknown as number; // multiple 10 in HB secs
    }
    private clearHeartbeat() { clearInterval(this.timerHeartbeat); }

    private updateSubscriptions() {
        if (this._connected && this._ready) {
            const cmSubscribe: types.ClientMessageSubscribe = { subscriptions: this._subscriptions };
            this.sendMessage('subscribe', cmSubscribe);
        }
    }

    private onObjData(object: object) {
        if (Object.keys(object).length !== 1) return;

        const cmd = Object.keys(object)[0] as types.BrokerMessageType;
        const parameters = Object.values(object)[0] as object;

        this.lastRcvHeartbeat = Date.now();

        switch (cmd) {
            case 'loginAck':
                const bmLoginAck = validateObject<types.BrokerMessageLoginAck>(types.BrokerMessageLoginAck, parameters);
                if (!bmLoginAck) return;
                this.onDataLoginAck(bmLoginAck);
                break;
            case 'heartbeat':
                if (validateObject<types.BrokerMessageHeartbeat>(types.BrokerMessageHeartbeat, parameters))
                    this.lastRcvHeartbeat = Date.now();
                break;
            case 'subscribeAck':
                const bmSubscribeAck = validateObject<types.BrokerMessageSubscribeAck>(types.BrokerMessageSubscribeAck, parameters);
                if (!bmSubscribeAck) return;
                this.onDataSubscribeAck(bmSubscribeAck);
                break;
            case 'publishAck':
                const bmPublishAck = validateObject<types.BrokerMessagePublishAck>(types.BrokerMessagePublishAck, parameters);
                if (!bmPublishAck) return;
                this.onDataPublishAck(bmPublishAck);
                break;
            case 'delivery':
                const bmDelivery = validateObject<types.BrokerMessageDelivery>(types.BrokerMessageDelivery, parameters);
                if (!bmDelivery) return;
                const ack = new EngineMqPublishDeliveryAck(bmDelivery.options.messageId, this.sendMessage);
                this.emit('mq-message', ack, bmDelivery.topic, bmDelivery.message, bmDelivery);
                break;
            case 'deliveryReport':
                const bmDeliveryReport = validateObject<types.BrokerMessageDeliveryReport>(types.BrokerMessageDeliveryReport, parameters);
                if (!bmDeliveryReport) return;
                this.emit('mq-delivery-report', bmDeliveryReport);
                break;
        }
    }

    private onDataLoginAck(bmLoginAck: types.BrokerMessageLoginAck) {
        if (bmLoginAck.errorMessage) {
            this._allowReconnect = false;
            this.emit('mq-error', 'LOGIN', bmLoginAck.errorMessage);
            this.destroy();
            return;
        }
        if (bmLoginAck.heartbeatSec) {
            this.initHeartbeat(bmLoginAck.heartbeatSec);
            this.setKeepAlive(true, bmLoginAck.heartbeatSec / 2 * 1000);
        }
        this._ready = true;
        this.emit('mq-ready');

        this.updateSubscriptions();
    }

    private publishAckWaitingList = new Map<string, PublishAckWaitingListItem>();
    private addToPublishAckWaitingList(messageId: string, resolver: AckResolver) {
        this.publishAckWaitingList.set(messageId, {
            resolver: resolver,
            waitingStartAt: Date.now()
        });
    }
    private maintainPublishAckWaitingListLastRun = 0;
    private maintainPublishAckWaitingList() {
        const expiredMs = ACK_WAITINGLIST_TIMEOUT_SEC * 1000;
        const frequencyMs = expiredMs / 2;
        const now = Date.now();

        if (now - this.maintainPublishAckWaitingListLastRun < frequencyMs)
            return;

        if (this.publishAckWaitingList.size < ACK_WAITINGLIST_MIN_LENGTH)
            return;

        for (const k of this.publishAckWaitingList.keys()) {
            const v = this.publishAckWaitingList.get(k);
            if (v && now - v.waitingStartAt > expiredMs)
                this.publishAckWaitingList.delete(k);
        }
        this.maintainPublishAckWaitingListLastRun = now;
    }
    private onDataSubscribeAck(bmSubscribeAck: types.BrokerMessageSubscribeAck) {
        const errors = bmSubscribeAck.errors.filter(error => error.errorMessage);
        if (errors.length === 0)
            return;

        this.emit('mq-error', 'SUBSCRIBE', 'Cannot subscribe to following topics', errors);
    }
    private onDataPublishAck(bmPublishAck: types.BrokerMessagePublishAck) {
        if (!bmPublishAck.messageId)
            return;

        const wlItem = this.publishAckWaitingList.get(bmPublishAck.messageId);
        this.publishAckWaitingList.delete(bmPublishAck.messageId);
        if (wlItem)
            wlItem.resolver(bmPublishAck.errorMessage);

        this.maintainPublishAckWaitingList();
    }
}

export type EngineMqClientParameters = {
    clientId?: string,
    host?: string,
    port?: number,
    authToken?: string,
    connectAutoStart?: boolean,
    maxWorkers?: number,
};
export const defaultEngineMqClientParameters: Required<EngineMqClientParameters> = {
    clientId: '',
    host: '127.0.0.1',
    port: 16_677,
    authToken: '',
    connectAutoStart: false,
    maxWorkers: 1,
};

export const MessageQos = types.MessageQos;
export const MessagePriority = types.MessagePriority;
export type EngineMqMessageDeliveryReport = types.BrokerMessageDeliveryReport;

export type EngineMqPublishClientOptions = {
    timeoutMs?: number,
};
export const defaultEngineMqPublishMessageOptions: types.ClientMessagePublishOptions = {
    messageId: '',
    qos: MessageQos.Normal,
    priority: MessagePriority.Normal,
    delayMs: 0,
    expirationMs: 0,
};
export const defaultEngineMqPublishClientOptions: Required<EngineMqPublishClientOptions> = {
    timeoutMs: 500,
};
export class EngineMqPublishDeliveryAck {
    private finalized = false;
    private messageId: string;
    private lastPercent = 0;
    private sendMessageFn: SendMessageFunction;

    constructor(messageId: string, sendMesage: SendMessageFunction) {
        this.messageId = messageId;
        this.sendMessageFn = sendMesage;
    }

    public start() {
        this.progress(0);
    }
    public progress(percent: number) {
        if (this.finalized)
            return;
        percent = Math.min(Math.max(percent, 0), 100);
        this.lastPercent = percent;
        const bmDeliveryAck: types.ClientMessageDeliveryAck = { messageId: this.messageId, percent: percent };
        this.sendMessageFn('deliveryAck', bmDeliveryAck);
        if (percent === 100)
            this.finalized = true;
    }
    public finish() {
        this.progress(100);
    }

    public resolve(value: object = {}) {
        if (this.finalized)
            return;

        this.lastPercent = 100;
        const bmDeliveryAck: types.ClientMessageDeliveryAck = { messageId: this.messageId, percent: this.lastPercent, resolveReason: value };
        this.sendMessageFn('deliveryAck', bmDeliveryAck);
        this.finalized = true;
    }

    public reject(reason: object = {}, retryDelayMs = 0) {
        if (this.finalized)
            return;
        const bmDeliveryAck: types.ClientMessageDeliveryAck = { messageId: this.messageId, percent: this.lastPercent, rejectReason: reason, rejectRetryDelayMs: retryDelayMs };
        this.sendMessageFn('deliveryAck', bmDeliveryAck);
        this.finalized = true;
    }

    public rejectFatal(reason: object = {}) {
        if (this.finalized)
            return;
        const bmDeliveryAck: types.ClientMessageDeliveryAck = { messageId: this.messageId, percent: this.lastPercent, rejectReason: reason, rejectRetryDelayMs: undefined };
        this.sendMessageFn('deliveryAck', bmDeliveryAck);
        this.finalized = true;
    }
}
