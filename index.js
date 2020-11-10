const { MongoClient } = require('mongodb');
const uri = require('mongodb-uri');
const zlib = require('zlib');

/**
 * Module constants.
 */

const noop = () => { }

const OPTIONS_LIST = [
    'port',
    'host',
    'username',
    'password',
    'database',
    'collection',
    'compression',
    'engine',
    'Promise',
    'delimiter',
    'prefix',
    'ttl',
    'count',
    'hosts',
    'options'
]

const createIndex = (db, coll) => db.createIndex(coll, { 'expireAt': 1 }, { expireAfterSeconds: 0 })

class MongoStore {
    /**
     * MongoStore constructor.
     *
     * @param {Object} options
     * @api public
     */

    constructor(conn, options = {}) {
        if ('object' === typeof conn) {
            if ('function' !== typeof conn.collection) {
                options = conn
                if (Object.keys(options).length === 0) {
                    conn = null
                } else if (options.client) {
                    this.client = options.client
                } else {
                    options.database = options.database || options.db
                    options.hosts = options.hosts || [
                        {
                            port: options.port || 27017,
                            host: options.host || '127.0.0.1'
                        }
                    ]
                    conn = uri.format(options)
                }
            } else {
                this.client = conn
            }
        }

        conn = conn || 'mongodb://127.0.0.1:27017'
        var coll = (this.coll = options.collection || 'cacheman')
        this.compression = options.compression || false
        this.ready = Promise.resolve().then(async () => {
            if ('string' === typeof conn) {
                const mongoOptions = OPTIONS_LIST.reduce((opt, key) => {
                    delete opt[key]
                    return opt
                }, { useNewUrlParser: true, useUnifiedTopology: true, ...options })

                const mongoClient = await MongoClient.connect(conn, mongoOptions)
                const db = mongoClient.db()

                this.closeMongoClient = mongoClient.close.bind(mongoClient)

                try{await createIndex(db, coll)}
                catch(e){}
                return this.client = db
            } else {
                if (this.client) {
                    try{await createIndex(this.client, coll)}
                    catch(e){}
                    return this.client
                }
            }
            throw new Error('Invalid mongo connection.')
        })
    }

    /**
     * Get an entry.
     *
     * @param {String} key
     * @param {Function} fn
     * @api public
     */

    get(key, fn = noop) {
        this.ready.then(async db => {
            const data = await db.collection(this.coll).findOne({ key: key })
            if (!data) return fn(null, null)
            //Mongo's TTL might have a delay, to fully respect the TTL, it is best to validate it in get.
            if (data.expireAt.getTime() < Date.now()) {
                this.del(key)
                return fn(null, null)
            }
            try {
                if (data.compressed) return decompress(data.value, fn)
                fn(null, data.value)
            } catch (err) {
                fn(err)
            }
        })
            .catch(err => fn(err))
    }

    /**
     * Set an entry.
     *
     * @param {String} key
     * @param {Mixed} val
     * @param {Number} ttl
     * @param {Function} fn
     * @api public
     */

    set(key, val, ttl, fn = noop) {
        if ('function' === typeof ttl) {
            fn = ttl
            ttl = null
        }

        let data
        let store = this
        let query = { key: key }
        let options = { upsert: true, safe: true }

        try {
            data = {
                key: key,
                value: val,
                expireAt: new Date(Date.now() + (ttl || 60) * 1000)
            }
        } catch (err) {
            return fn(err)
        }

        this.ready.then(db => {
            function update($set) {
                db.collection(store.coll).updateOne(query, { $set }, options, (err, data) => {
                    if (err) return fn(err)
                    if (!data) return fn(null, null)
                    fn(null, val)
                })
            }
            if (!this.compression) {
                update(data)
            } else {
                compress(data, function compressData(err, data) {
                    if (err) return fn(err)
                    update(data)
                })
            }
        })
            .catch(err => fn(err))
    }

    /**
     * Delete an entry.
     *
     * @param {String} key
     * @param {Function} fn
     * @api public
     */

    del(key, fn = noop) {
        this.ready.then(db => {
            db.collection(this.coll).removeOne({ key: key }, { safe: true }, fn)
        })
            .catch(err => fn(err))
    }

    /**
     * Clear all entries for this bucket.
     *
     * @param {Function} fn
     * @api public
     */

    clear(fn = noop) {
        this.ready.then(db => {
            db.collection(this.coll).removeOne({}, { safe: true }, fn)
        })
            .catch(err => fn(err))
    }
}

/**
 * Non-exported Helpers
 */

/**
 * Compress data value.
 *
 * @param {Object} data
 * @param {Function} fn
 * @api public
 */

function compress(data, fn) {
    // Data is not of a "compressable" type (currently only Buffer)
    if (!Buffer.isBuffer(data.value)) return fn(null, data)

    zlib.gzip(data.value, (err, val) => {
        // If compression was successful, then use the compressed data.
        // Otherwise, save the original data.
        if (!err) {
            data.value = val
            data.compressed = true
        }
        fn(err, data)
    })
}

/**
 * Decompress data value.
 *
 * @param {Object} value
 * @param {Function} fn
 * @api public
 */

function decompress(value, fn) {
    let v = value.buffer && Buffer.isBuffer(value.buffer) ? value.buffer : value
    zlib.gunzip(v, fn)
}
module.exports = MongoStore;