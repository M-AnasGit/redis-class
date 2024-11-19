const redis = require('redis')

class HttpError extends Error {
	constructor(message, statusCode, error) {
		super(message)
		this.statusCode = statusCode
		this.name = this.constructor.name
		this.error = error
	}
}

class ElastiCache {
	/**
	 * Initializes the ElastiCache instance with a Redis client configuration.
	 * @param {Object} redisConfig - Configuration object for connecting to Redis.
	 * @param {boolean} [dev=false] - If true, logs will be shown for method calls (for development purposes).
	 */

	constructor(redisConfig, dev = false) {
		this.client = redis.createClient(redisConfig)
		this.dev = dev
	}

	/**
	 * Connects to the Redis server.
	 * @throws {HttpError} Throws error if the connection fails.
	 * @returns {Promise<void>} Resolves when the connection is successful.
	 */
	async connect() {
		try {
			await this.client.connect()
			if (this.dev) console.log('Connected to Redis')
		} catch (err) {
			if (this.dev) console.error('Redis connection failed', err)
			throw new HttpError('Redis connection failed', 500, err)
		}
	}

	/**
	 * Disconnects from the Redis server.
	 * @throws {HttpError} Throws error if the disconnection fails.
	 * @returns {Promise<void>} Resolves when the disconnection is successful.
	 */
	async disconnect() {
		try {
			await this.client.disconnect()
			if (this.dev) console.log('Disconnected from Redis')
		} catch (err) {
			if (this.dev) console.error('Redis disconnection failed', err)
			throw new HttpError('Redis disconnection failed', 500)
		}
	}

	/**
	 * Sets a key-value pair in Redis. Optionally with TTL (Time To Live).
	 * @param {string} key - The Redis key.
	 * @param {any} value - The value to be stored.
	 * @param {number|null} ttl - The TTL in seconds (optional).
	 * @throws {HttpError} Throws error if setting the value fails.
	 * @returns {Promise<void>} Resolves when the value is successfully set.
	 */
	async set(key, value, ttl = null) {
		try {
			const options = ttl ? { EX: ttl } : undefined
			await this.client.set(key, JSON.stringify(value), options)
			if (this.dev) console.log(`Set value for key: ${key}`)
		} catch (err) {
			if (this.dev) console.error('Error setting value in Redis:', err)
			throw new HttpError('Error saving to Redis', 500)
		}
	}

	/**
	 * Gets the value of a given key from Redis.
	 * @param {string} key - The Redis key.
	 * @param {boolean} [parse=false] - If true, parses the JSON string value.
	 * @throws {HttpError} Throws error if the key is not found or fetching fails.
	 * @returns {Promise<any>} The value associated with the key.
	 */
	async get(key, parse = false) {
		try {
			const value = await this.client.get(key)
			if (value === null) {
				if (this.dev) console.log(`Key not found: ${key}`)
				throw new HttpError('Key not found', 404)
			}

			return parse ? JSON.parse(value) : value
		} catch (err) {
			if (this.dev) console.error('Error getting value from Redis:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error fetching from Redis', 500)
		}
	}

	/**
	 * Deletes a key from Redis.
	 * @param {string} key - The Redis key to delete.
	 * @throws {HttpError} Throws error if the key is not found or deletion fails.
	 * @returns {Promise<void>} Resolves when the key is successfully deleted.
	 */
	async delete(key) {
		try {
			const result = await this.client.del(key)
			if (result === 0) {
				if (this.dev) console.log(`Key not found: ${key}`)
				throw new HttpError('Key not found', 404)
			}
			if (this.dev) console.log(`Deleted key: ${key}`)
		} catch (err) {
			if (this.dev) console.error('Error deleting from Redis:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error deleting from Redis', 500)
		}
	}

	/**
	 * Pushes a value to a Redis list.
	 * @param {string} key - The Redis list key.
	 * @param {any} value - The value to push to the list.
	 * @throws {HttpError} Throws error if pushing fails.
	 * @returns {Promise<void>} Resolves when the value is successfully pushed to the list / creates a list if it doesn't exist.
	 */
	async listPush(key, value) {
		try {
			await this.client.rPush(key, JSON.stringify(value))
			if (this.dev) console.log(`Pushed value to list: ${key}`)
		} catch (err) {
			if (this.dev) console.error('Error pushing to Redis list:', err)
			throw new HttpError('Error pushing to Redis list', 500)
		}
	}

	/**
	 * Pops a value from a Redis list.
	 * @param {string} key - The Redis list key.
	 * @param {boolean} [parse=false] - If true, parses the JSON string value.
	 * @throws {HttpError} Throws error if the list is empty or pop fails.
	 * @returns {Promise<any>} The value popped from the list.
	 */
	async listPop(key, parse = false) {
		try {
			const value = await this.client.rPop(key)
			if (value === null) {
				if (this.dev) console.log(`List is empty: ${key}`)
				throw new HttpError('List is empty', 404)
			}
			return parse ? JSON.parse(value) : value
		} catch (err) {
			if (this.dev) console.error('Error popping from Redis list:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error popping from Redis list', 500)
		}
	}

	/**
	 * Removes a specific value from a Redis list.
	 * @param {string} key - The Redis list key.
	 * @param {any} value - The value to remove from the list.
	 * @throws {HttpError} Throws error if the value is not found in the list.
	 * @returns {Promise<void>} Resolves when the value is successfully removed.
	 */
	async listRemove(key, value) {
		try {
			const removed = await this.client.lRem(key, 0, JSON.stringify(value))
			if (removed === 0) {
				if (this.dev) console.log(`Value not found in list: ${key}`)
				throw new HttpError('Value not found in list', 404)
			}
			if (this.dev) console.log(`Removed value from list: ${key}`)
		} catch (err) {
			if (this.dev) console.error('Error removing from Redis list:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error removing from Redis list', 500)
		}
	}

	/**
	 * Gets values from a Redis list within a specified range.
	 * @param {string} key - The Redis list key.
	 * @param {number} [start=0] - The start index (inclusive).
	 * @param {number} [end=-1] - The end index (inclusive, -1 for all).
	 * @param {boolean} [parse=false] - If true, parses the JSON string values.
	 * @throws {HttpError} Throws error if fetching the values fails or the list is empty.
	 * @returns {Promise<any[]>} The list of values in the specified range.
	 */
	async listGet(key, start = 0, end = -1, parse = false) {
		try {
			const values = await this.client.lRange(key, start, end)
			if (values.length === 0) {
				if (this.dev) console.log(`List is empty: ${key}`)
				throw new HttpError('List is empty', 404)
			}
			return parse ? values.map((v) => JSON.parse(v)) : values
		} catch (err) {
			if (this.dev) console.error('Error fetching from Redis list:', err)
			throw new HttpError('Error fetching from Redis list', 500)
		}
	}

	/**
	 * Retrieves all keys matching a prefix and their values.
	 * @param {string} prefix - The prefix to match keys.
	 * @throws {HttpError} Throws error if no keys match or fetching fails.
	 * @returns {Promise<Object[]>} An array of objects with key-value pairs.
	 */
	async getByPrefix(prefix) {
		try {
			const keys = await this.client.keys(`${prefix}:*`)
			if (keys.length === 0) {
				if (this.dev) console.log(`Index not found with prefix: ${prefix}`)
				throw new HttpError('Index not found', 404)
			}
			const values = await Promise.all(keys.map((key) => this.get(key, true)))
			return keys.map((key, index) => ({ [key]: values[index] }))
		} catch (err) {
			if (this.dev) console.error('Error fetching keys by prefix:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error fetching keys by prefix', 500)
		}
	}

	/**
	 * Sets a value for a specific field in a Redis hash.
	 * @param {string} key - The Redis hash key.
	 * @param {string} field - The field name within the hash.
	 * @param {any} value - The value to set for the field.
	 * @throws {HttpError} Throws error if setting the hash value fails.
	 * @returns {Promise<void>} Resolves when the value is successfully set / creates a hash if it doesn't exist.
	 */
	async hashSet(key, field, value) {
		try {
			await this.client.hSet(key, field, JSON.stringify(value))
			if (this.dev) console.log(`Set value for field: ${field}`)
		} catch (err) {
			if (this.dev) console.error('Error saving to Redis hash:', err)
			throw new HttpError('Error saving to Redis hash', 500)
		}
	}

	/**
	 * Gets a value for a specific field in a Redis hash.
	 * @param {string} key - The Redis hash key.
	 * @param {string} field - The field name within the hash.
	 * @param {boolean} [parse=false] - If true, parses the JSON string value.
	 * @throws {HttpError} Throws error if the field is not found or fetching fails.
	 * @returns {Promise<any>} The value of the field in the hash.
	 */
	async hashGet(key, field, parse = false) {
		try {
			const value = await this.client.hGet(key, field)
			if (value === null) {
				if (this.dev) console.log(`Field not found: ${field}`)
				throw new HttpError('Field not found', 404)
			}
			return parse ? JSON.parse(value) : value
		} catch (err) {
			if (this.dev) console.error('Error fetching from Redis hash:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error fetching from Redis hash', 500)
		}
	}

	/**
	 * Gets all fields and values from a Redis hash.
	 * @param {string} key - The key of the hash.
	 * @throws {HttpError} Throws error if fetching the hash fails.
	 * @returns {Promise<Object>} An object with all field-value pairs from the hash.
	 */
	async hashGetAll(key, parse = false) {
		try {
			const value = await this.client.hGetAll(key)
			if (value === null) {
				if (this.dev) console.log(`Field not found: ${field}`)
				throw new HttpError('Field not found', 404)
			}
			return parse ? Object.fromEntries(Object.entries(value).map(([k, v]) => [k, JSON.parse(v)])) : value
		} catch (error) {
			if (this.dev) console.error('Error finding the Redis hash:', error)
			if (error instanceof HttpError) throw error
			throw new HttpError('Error finding the Redis hash', 500)
		}
	}

	/**
	 * Deletes a field from a Redis hash.
	 * @param {string} key - The key of the hash.
	 * @param {string} field - The field to delete from the hash.
	 * @throws {HttpError} Throws error if the field does not exist or if deletion fails.
	 * @returns {Promise<void>} Resolves when the field is deleted.
	 */
	async hashDelete(key, field) {
		try {
			const result = await this.client.hDel(key, field)
			if (result === 0) {
				if (this.dev) console.log(`Field not found: ${field}`)
				throw new HttpError('Field not found', 404)
			}
			if (this.dev) console.log(`Deleted field: ${field}`)
		} catch (err) {
			if (this.dev) console.error('Error deleting from Redis hash:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error deleting from Redis hash', 500)
		}
	}

	/**
	 * Refreshes the time-to-live (TTL) for a given key in Redis.
	 *
	 * @param {string} key - The key to refresh the time-to-live for.
	 * @param {number} ttl - The time-to-live in seconds.
	 *
	 * @returns {Promise<void>} Resolves when the key is successfully refreshed.
	 *
	 * @throws {HttpError} Throws error if the key does not exist or if refreshing fails.
	 */
	async refreshKey(key, ttl) {
		try {
			const result = await this.client.expire(key, ttl)
			if (result === 0) {
				if (this.dev) console.log(`Key not found: ${key}`)
				throw new HttpError('Key not found', 404)
			}
			if (this.dev) console.log(`Refreshed key: ${key}`)
		} catch (error) {
			if (this.dev) console.error('Error refreshing key in Redis:', error)
			if (error instanceof HttpError) throw error
		}
	}

	/**
	 * Retrieves all keys matching a pattern from Redis.
	 * @param {string} [pattern='*'] - The pattern to match the keys against.
	 * @param {boolean} [parse=false] - If true, the values will be parsed as JSON.
	 * @throws {HttpError} Throws error if fetching the keys or values fails.
	 * @returns {Promise<Array<Object>>} An array of objects with key-value pairs.
	 */
	async getAll() {
		try {
			const keys = await this.client.keys('*')
			if (keys.length === 0) {
				if (this.dev) console.log('No keys found')
				throw new HttpError('No keys found', 404)
			}

			const values = []
			for (const key of keys) {
				const type = await this.client.type(key)
				if (type === 'string') {
					values.push({ [key]: await this.get(key, true) })
					continue
				} else if (type === 'list') {
					values.push({ [key]: await this.listGet(key, 0, -1, true) })
					continue
				} else if (type === 'hash') {
					values.push({ [key]: await this.hashGetAll(key, true) })
				}
			}
			return values
		} catch (err) {
			if (this.dev) console.error('Error fetching all keys from Redis:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error fetching all keys from Redis', 500)
		}
	}

	/**
	 * Retrieves the time-to-live (TTL) for a given key in Redis.
	 * @param {string} key - The key to check the TTL for.
	 * @throws {HttpError} Throws error if the key does not exist or if fetching the TTL fails.
	 * @returns {Promise<number>} The TTL of the key in seconds.
	 */
	async getTimeToLive(key) {
		try {
			const ttl = await this.client.ttl(key)
			if (ttl === -2) {
				if (this.dev) console.log(`Key not found: ${key}`)
				throw new HttpError('Key not found', 404)
			}
			return ttl
		} catch (err) {
			if (this.dev) console.error('Error fetching TTL from Redis:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error fetching TTL from Redis', 500)
		}
	}

	/**
	 * Deletes all keys matching a pattern from Redis.
	 * @param {string} [pattern='*'] - The pattern to match the keys against.
	 * @throws {HttpError} Throws error if deletion fails or no keys match the pattern.
	 * @returns {Promise<void>} Resolves when all matching keys are deleted.
	 */
	async deleteAll(pattern = '*') {
		try {
			const keys = await this.client.keys(pattern)
			if (keys.length === 0) {
				if (this.dev) console.log('No keys found')
				throw new HttpError('No keys found', 404)
			}
			await Promise.all(keys.map((key) => this.delete(key)))
			if (this.dev) console.log('Deleted all keys matching pattern:', pattern)
		} catch (err) {
			if (this.dev) console.error('Error deleting all keys from Redis:', err)
			if (err instanceof HttpError) throw err
			throw new HttpError('Error deleting all keys from Redis', 500)
		}
	}
}

module.exports = ElastiCache
