const ElastiCache = require('./Elasticache')

describe('ElastiCache', () => {
	let cache

	beforeAll(async () => {
		cache = new ElastiCache({
			host: 'localhost',
			port: 6379,
		})
		await cache.connect()
	})

	afterAll(async () => {
		try {
			await cache.deleteAll()
		} catch (error) {
			console.error('Error cleaning up:', error)
		} finally {
			await cache.disconnect()
		}
	})

	describe('Key-Value Operations', () => {
		test('set and get', async () => {
			await cache.set('test:key1', { name: 'John' })
			const value = await cache.get('test:key1', true)
			expect(value).toEqual({ name: 'John' })
		})

		test('set with TTL', async () => {
			await cache.set('test:key2', 'temporary', 1)
			await new Promise((resolve) => setTimeout(resolve, 1100))
			await expect(cache.get('test:key2')).rejects.toThrow('Key not found')
		})

		test('delete', async () => {
			await cache.set('test:key3', 'to be deleted')
			await cache.delete('test:key3')
			await expect(cache.get('test:key3')).rejects.toThrow('Key not found')
		})
	})

	describe('List Operations', () => {
		test('listPush and listPop', async () => {
			await cache.listPush('test:list1', 'item1')
			await cache.listPush('test:list1', 'item2')
			const item = await cache.listPop('test:list1', true)
			expect(item).toBe('item2')
		})

		test('listRemove', async () => {
			await cache.listPush('test:list2', 'keep')
			await cache.listPush('test:list2', 'remove')
			await cache.listRemove('test:list2', 'remove')
			const items = await cache.listGet('test:list2', 0, -1, true)
			expect(items).toEqual(['keep'])
		})

		test('listGet', async () => {
			await cache.listPush('test:list3', 'first')
			await cache.listPush('test:list3', 'second')
			await cache.listPush('test:list3', 'third')
			const items = await cache.listGet('test:list3', 1, 2, true)
			expect(items).toEqual(['second', 'third'])
		})
	})

	describe('Aggregation Support', () => {
		test('getByPrefix', async () => {
			await cache.set('users:1', { name: 'Alice' })
			await cache.set('users:2', { name: 'Bob' })
			const users = await cache.getByPrefix('users')
			expect(users).toHaveLength(2)
			expect(users[0]['users:1'].name).toBe('Alice')
			expect(users[1]['users:2'].name).toBe('Bob')
		})
	})

	describe('Hash Operations', () => {
		test('hashSet and hashGet', async () => {
			await cache.hashSet('test:hash1', 'field1', { value: 'test' })
			const value = await cache.hashGet('test:hash1', 'field1', true)
			expect(value).toEqual({ value: 'test' })
		})

		test('hashDelete', async () => {
			await cache.hashSet('test:hash2', 'field1', 'to be deleted')
			await cache.hashDelete('test:hash2', 'field1')
			await expect(cache.hashGet('test:hash2', 'field1')).rejects.toThrow('Field not found')
		})
	})

	describe('Utility Methods', () => {
		test('getTimeToLive', async () => {
			await cache.set('test:ttl', 'expiring soon', 10)
			const ttl = await cache.getTimeToLive('test:ttl')
			expect(ttl).toBeGreaterThan(0)
			expect(ttl).toBeLessThanOrEqual(10)
		})

		test('getAll', async () => {
			await cache.set('test:getall1', 'value1')
			await cache.set('test:getall2', 'value2')
			const allValues = await cache.getAll()
			console.log(allValues)
			expect(allValues.find((v) => v['test:getall1'] === 'value1')).toBeTruthy()
			expect(allValues.find((v) => v['test:getall2'] === 'value2')).toBeTruthy()
		})

		test('deleteAll', async () => {
			await cache.set('test:deleteall1', 'value1')
			await cache.set('test:deleteall2', 'value2')
			await cache.deleteAll('test:deleteall*')
			await expect(cache.get('test:deleteall1')).rejects.toThrow('Key not found')
			await expect(cache.get('test:deleteall2')).rejects.toThrow('Key not found')
		})
	})

	describe('Error Handling', () => {
		test('get non-existent key', async () => {
			await expect(cache.get('non:existent')).rejects.toThrow('Key not found')
		})

		test('delete non-existent key', async () => {
			await expect(cache.delete('non:existent')).rejects.toThrow('Key not found')
		})

		test('pop from empty list', async () => {
			await expect(cache.listPop('empty:list')).rejects.toThrow('List is empty')
		})

		test('remove non-existent value from list', async () => {
			await cache.listPush('test:list4', 'existing')
			await expect(cache.listRemove('test:list4', 'non-existent')).rejects.toThrow('Value not found in list')
		})

		test('get by non-existent prefix', async () => {
			await expect(cache.getByPrefix('non:existent')).rejects.toThrow('Index not found')
		})

		test('get non-existent hash field', async () => {
			await cache.hashSet('test:hash3', 'existing', 'value')
			await expect(cache.hashGet('test:hash3', 'non-existent')).rejects.toThrow('Field not found')
		})

		test('delete non-existent hash field', async () => {
			await expect(cache.hashDelete('test:hash3', 'non-existent')).rejects.toThrow('Field not found')
		})

		test('get TTL of non-existent key', async () => {
			await expect(cache.getTimeToLive('non:existent')).rejects.toThrow('Key not found')
		})

		test('deleteAll with no matching keys', async () => {
			await expect(cache.deleteAll('non:existent:*')).rejects.toThrow('No keys found')
		})
	})
})
