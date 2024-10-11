import duckdb from "duckdb";
import * as genericPool from "generic-pool";
import util from "util";


class RecyclingPool<T> extends genericPool.Pool<T> {
    private config: { recycleTimeoutMillis?: number, recycleTimeoutJitter?: number } = {};

    constructor(factory: Factory<T>, options: Options) {
        // @ts-ignore
        super(genericPool.DefaultEvictor, genericPool.Deque, genericPool.PriorityQueue, factory, options);
        // New config option for when to recycle a non-idle connection
        this.config['recycleTimeoutMillis'] = (typeof options.recycleTimeoutMillis == 'undefined') ? 900000 : options.recycleTimeoutMillis;
        this.config['recycleTimeoutJitter'] = (typeof options.recycleTimeoutJitter == 'undefined') ? 60000 : options.recycleTimeoutJitter;
        console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), 'Creating a RecyclingPool');
    }

    override release(resource: T) {
        // @ts-ignore
        const loan = this._resourceLoans.get(resource);
        const creationTime = typeof loan == 'undefined' ? 0 : loan.pooledResource.creationTime;

        // If the connection has been in use for longer than the recycleTimeoutMillis, then destroy it instead of releasing it back into the pool.
        // If that deletion brings the pool size below the min, a new connection will automatically be created within the destroy method.
        if (new Date(creationTime + this.config.recycleTimeoutMillis - (Math.random() * this.config.recycleTimeoutJitter!)) <= new Date()) {
            console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), 'pre-destroy  pool status: min:', this.min, 'max:', this.max, 'size', this.size, 'pending:', this.pending, 'borrowed:', this.borrowed, 'available:', this.available);
            return this.destroy(resource);
        }
        console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), 'pre-release  pool status: min:', this.min, 'max:', this.max, 'size', this.size, 'pending:', this.pending, 'borrowed:', this.borrowed, 'available:', this.available);
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

const createFactory = ({ dbUrl, accessMode = "read_write" }: { dbUrl: string, accessMode?: "read_only" | "read_write" }) => {
    const factory = {
        create: async function () {
            console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), "Creating a connection");

            const connection = new duckdb.Database(dbUrl, {
                "access_mode": accessMode,
                "max_memory": "512MB",
                "threads": "4"
            }, (err) => {
                if (err) {
                    console.error(err);
                }
            });

            console.log("accessMode:", accessMode)
            // Run any connection initialization commands here
            connection.all("SET THREADS='1';")
            console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), "Connection");

            return connection;
        },
        destroy: async function (connection: duckdb.Database) {
            console.log(new Date().toJSON().replace('T', ' ').replace('Z', ' '), "Destroying a connection");
            return connection.close();
        }
    };

    return factory;
}

function methodPromisify<T extends object, R>(
    methodFn: (...args: any[]) => any
): (target: T, ...args: any[]) => Promise<R> {
    return util.promisify((target: T, ...args: any[]): any =>
        methodFn.bind(target)(...args)
    ) as any;
}

const dbAllAsync = methodPromisify<duckdb.Database, duckdb.TableData>(
    duckdb.Database.prototype.all
);

function linkPool(pool: RecyclingPool<duckdb.Database>) {
    return async function runQuery(strings: TemplateStringsArray, ...exprs: (string | number)[]) {
        // gets connection from pool, runs query, release connection

        // console.log(strings, exprs);
        const conn = await pool.acquire();

        const result = await dbAllAsync(conn, strings[0]!, ...exprs);

        await pool.release(conn);
        return result;

        // conn.all(strings[0]!, function (err, res) {
        //     if (err) {
        //         console.warn(err);
        //         return;
        //     }

        //     pool.release(conn);
        //     // console.log(res)
        //     return res;
        // });

    }
}

export function mrduck({ dbUrl, min = 0, max = 1, accessMode = "read_write" }: { dbUrl: string, min?: number, max?: number, accessMode?: "read_only" | "read_write" }) {
    // create pool using min, max parameters

    const factory = createFactory({ dbUrl, accessMode });
    const opts = {
        max, // maximum size of the pool
        min // minimum size of the pool
    };

    const pool = createRecyclingPool<duckdb.Database>(factory, opts);

    return linkPool(pool);
}
