import duckdb from 'duckdb';
import * as genericPool from 'generic-pool';

export class RecyclingPool<T> extends genericPool.Pool<T> {
	private config: { recycleTimeoutMillis?: number; recycleTimeoutJitter?: number } = {};

	constructor(factory: Factory<T>, options: Options) {
		// @ts-ignore
		super(genericPool.DefaultEvictor, genericPool.Deque, genericPool.PriorityQueue, factory, options);
		// New config option for when to recycle a non-idle connection
		this.config['recycleTimeoutMillis'] = (options.recycleTimeoutMillis === undefined)
			? 900000
			: options.recycleTimeoutMillis;
		this.config['recycleTimeoutJitter'] = (options.recycleTimeoutJitter === undefined)
			? 60000
			: options.recycleTimeoutJitter;
		// console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), 'Creating a RecyclingPool');
	}

	override release(resource: T) {
		// @ts-ignore
		const loan = this._resourceLoans.get(resource);
		const creationTime = loan === undefined ? 0 : loan.pooledResource.creationTime;

		// If the connection has been in use for longer than the recycleTimeoutMillis, then destroy it instead of releasing it back into the pool.
		// If that deletion brings the pool size below the min, a new connection will automatically be created within the destroy method.
		if (
			new Date(creationTime + this.config.recycleTimeoutMillis - (Math.random() * this.config.recycleTimeoutJitter!))
				<= new Date()
		) {
			// console.log(
			// 	new Date().toJSON().replace('T', ' ').replace('Z', ' '),
			// 	'pre-destroy  pool status: min:',
			// 	this.min,
			// 	'max:',
			// 	this.max,
			// 	'size',
			// 	this.size,
			// 	'pending:',
			// 	this.pending,
			// 	'borrowed:',
			// 	this.borrowed,
			// 	'available:',
			// 	this.available,
			// );
			return this.destroy(resource);
		}
		// console.log(
		// 	new Date().toJSON().replace('T', ' ').replace('Z', ' '),
		// 	'pre-release  pool status: min:',
		// 	this.min,
		// 	'max:',
		// 	this.max,
		// 	'size',
		// 	this.size,
		// 	'pending:',
		// 	this.pending,
		// 	'borrowed:',
		// 	this.borrowed,
		// 	'available:',
		// 	this.available,
		// );
		return super.release(resource);
	}
}

interface Factory<T> {
	create(): Promise<T>;
	destroy(connection: T): Promise<void>;
	validate?(connection: T): Promise<boolean>;
}

interface Options {
	max?: number;
	min?: number;
	maxWaitingClients?: number;
	testOnBorrow?: boolean;
	testOnReturn?: boolean;
	acquireTimeoutMillis?: number;
	fifo?: boolean;
	priorityRange?: number;
	autostart?: boolean;
	evictionRunIntervalMillis?: number;
	numTestsPerEvictionRun?: number;
	softIdleTimeoutMillis?: number;
	idleTimeoutMillis?: number;
	recycleTimeoutMillis?: number;
	recycleTimeoutJitter?: number;
}

// Equivalent to createPool function from generic-pool
export function createRecyclingPool<T>(factory: Factory<T>, config: Options) {
	return new RecyclingPool<T>(factory, config);
}

export const createFactory = (
	{ dbUrl, accessMode = 'read_write' }: { dbUrl: string; accessMode?: 'read_only' | 'read_write' },
) => {
	const factory = {
		create: async function() {
			// console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), 'Creating a connection');

			const connection = new duckdb.Database(dbUrl, {
				access_mode: accessMode,
				max_memory: '512MB',
				threads: '4',
			}, (err) => {
				if (err) {
					console.error(err);
				}
			});

			// console.log('accessMode:', accessMode);
			// Run any connection initialization commands here
			connection.all("SET THREADS='1';");
			// console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), 'Connection');

			return connection;
		},
		destroy: async function(connection: duckdb.Database) {
			// console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), 'Destroying a connection');
			return connection.close();
		},
	};

	return factory;
};
