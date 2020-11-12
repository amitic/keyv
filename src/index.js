'use strict';

const EventEmitter = require('events');
const JSONB = require('json-buffer');

const loadStore = opts => {
	const adapters = {
		redis: '@keyv/redis',
		mongodb: '@keyv/mongo',
		mongo: '@keyv/mongo',
		sqlite: '@keyv/sqlite',
		postgresql: '@keyv/postgres',
		postgres: '@keyv/postgres',
		mysql: '@keyv/mysql'
	};
	if (opts.adapter || opts.uri) {
		const adapter = opts.adapter || /^[^:]*/.exec(opts.uri)[0];
		return new (require(adapters[adapter]))(opts);
	}

	return new Map();
};

async function concurrentMap(items, func, { concurrency } = {}) {
	if (!concurrency || concurrency < 0) {
		return Promise.all(
			(await items)
				.map(async (el, idx) => func(await el, idx))
		);
	}

	const results = [];
	const resolvedItems = await items;
	return Promise.all(
		new Array(Math.min(resolvedItems.length, concurrency))
			.fill(resolvedItems.entries())
			.map(async iterator => {
				for (const [idx, el] of iterator) {
					// eslint-disable-next-line no-await-in-loop
					const result = await func(await el, idx);
					results.push({ idx, result });
				}
			})
	).then(() => results.sort((a, b) => a.idx - b.idx).map(el => el.result));
}

async function zip(a, b) {
	return a.map((k, i) => [k, b[i]]);
}

class Keyv extends EventEmitter {
	constructor(uri, opts) {
		super();
		this.opts = Object.assign(
			{
				namespace: 'keyv',
				serialize: JSONB.stringify,
				deserialize: JSONB.parse
			},
			(typeof uri === 'string') ? { uri } : uri,
			opts
		);

		if (!this.opts.store) {
			const adapterOpts = Object.assign({}, this.opts);
			this.opts.store = loadStore(adapterOpts);
		}

		if (typeof this.opts.store.on === 'function') {
			this.opts.store.on('error', err => this.emit('error', err));
		}

		this.opts.store.namespace = this.opts.namespace;
	}

	_getKeyPrefix(key) {
		return `${this.opts.namespace}:${key}`;
	}

	_getPostprocess(key, data, opts) {
		return Promise.resolve(data)
			.then(data => {
				return (typeof data === 'string') ? this.opts.deserialize(data) : data;
			})
			.then(async data => {
				if (data === undefined) {
					return undefined;
				}

				if (typeof data.expires === 'number' && Date.now() > data.expires) {
					await this.delete(key);
					return undefined;
				}

				return (opts && opts.raw) ? data : data.value;
			});
	}

	get(key, opts) {
		const keyPrefixed = this._getKeyPrefix(key);
		const { store } = this.opts;
		return Promise.resolve()
			.then(() => store.get(keyPrefixed))
			.then(data => this._getPostprocess(key, data, opts));
	}

	mget(keys, opts) {
		const keysPrefixed = keys.map(this._getKeyPrefix.bind(this));
		const { store } = this.opts;
		const concurrency = (opts && opts.concurrency) || this.opts.concurrency;
		return Promise.resolve()
			.then(() => {
				if (store.mget) {
					return store.mget(
						keysPrefixed,
						{ concurrency }
					);
				}

				return concurrentMap(
					keysPrefixed,
					store.get.bind(store),
					{ concurrency }
				);
			})
			.then(data => concurrentMap(
				zip(keys, data),
				([key, d]) => this._getPostprocess(key, d, opts),
				{ concurrency }
			));
	}

	_setPreprocess(value, ttl) {
		if (typeof ttl === 'undefined') {
			ttl = this.opts.ttl;
		}

		if (ttl === 0) {
			ttl = undefined;
		}

		const expires = (typeof ttl === 'number') ? (Date.now() + ttl) : null;
		value = { value, expires };
		return this.opts.serialize(value);
	}

	set(key, value, ttl) {
		const keyPrefixed = this._getKeyPrefix(key);
		const { store } = this.opts;
		return Promise.resolve()
			.then(() => this._setPreprocess(value, ttl))
			.then(value => store.set(keyPrefixed, value, ttl))
			.then(() => true);
	}

	mset(keys, values, ttl, opts = {}) {
		if (!Array.isArray(keys)) {
			[keys, values, ttl, opts] = [
				Object.keys(keys),
				Object.values(keys),
				values,
				ttl
			];
		}

		if (!opts && typeof ttl === 'object') {
			[ttl, opts] = [null, ttl];
		}

		const keysPrefixed = keys.map(this._getKeyPrefix.bind(this));
		const { store } = this.opts;
		const concurrency = (opts && opts.concurrency) || this.opts.concurrency;
		return Promise.resolve(values)
			.then(values => Promise.all(values.map(v => this._setPreprocess(v, ttl))))
			.then(values => {
				if (store.mset) {
					return store.mset(
						keysPrefixed,
						values,
						ttl,
						{ concurrency }
					);
				}

				return concurrentMap(
					zip(keysPrefixed, values),
					([key, value]) => store.set(key, value, ttl),
					{ concurrency }
				);
			})
			.then(() => true);
	}

	delete(key) {
		const keyPrefixed = this._getKeyPrefix(key);
		const { store } = this.opts;
		return Promise.resolve()
			.then(() => store.delete(keyPrefixed));
	}

	clear() {
		const { store } = this.opts;
		return Promise.resolve()
			.then(() => store.clear());
	}
}

module.exports = Keyv;
