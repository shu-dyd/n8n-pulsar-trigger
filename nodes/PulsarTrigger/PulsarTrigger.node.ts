import type {
	ITriggerFunctions,
	INodeType,
	INodeTypeDescription,
	ITriggerResponse,
} from 'n8n-workflow';
import {NodeConnectionType, INodeExecutionData} from 'n8n-workflow';
import Pulsar, {AuthenticationToken} from "pulsar-client";

export class PulsarTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Pulsar Trigger',
		name: 'pulsarTrigger',
		icon: 'file:pulsar.svg',
		group: ['trigger'],
		version: 1,
		description: 'Consume messages from a pulsar topic',
		defaults: {
			name: 'Pulsar Trigger',
		},
		inputs: [],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'pulsarTriggerApi',
				required: true,
			},
		],
		properties: [
			// Resources and operations will go here
			{
				displayName: 'Topic',
				name: 'topic',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'topic-name',
				description: 'Name of the queue of topic to consume from',
			},
			{
				displayName: 'Broker',
				name: 'broker',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'broker-ip',
				description: 'Ip of the pulsar broker',
			},
			{
				displayName: 'Token',
				name: 'token',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'token',
				description: 'Authentication token for Pulsar',
			}
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const topic = this.getNodeParameter('topic') as string;
		const url = this.getNodeParameter('broker') as string;
		const token = this.getNodeParameter('token') as string;

		const tokenReal = new AuthenticationToken({
			token: token
		});
		const client = new Pulsar.Client({
			serviceUrl: url,
			authentication: tokenReal,
			operationTimeoutSeconds: 30
		});
		const consumer = await client.subscribe({
			topic: topic,
			subscription: 'n8n-pulsar',
			subscriptionType: 'Shared' // Use shared subscription for multiple consumers if needed
		});
		async function closeFunction() {
			await consumer.close();
			await client.close();
		}

		const startConsumer = async () => {
			while(true){

				const msg = await consumer.receive();
				const messageData = msg.getData().toString();
				consumer.acknowledge(msg);
				// Push the message to n8n workflow
				const outputData: INodeExecutionData[] = [{
					json: {
						message: messageData, // You can modify this to extract more data if needed
					},
				}];

				// Emit the message to the n8n workflow
				this.emit([outputData]);

			}
		}
		await startConsumer();
		return { closeFunction };
	}
}
