let bluebird = require('bluebird')
let logSystem = 'redisService'
require('./exceptionWriter.js')(logSystem)
const redis = require('redis')

const redisDB = (config.redis.db && config.redis.db > 0) ? config.redis.db : 0
bluebird.promisifyAll(redis)
let repository

function init() {
    repository = redis.createClient(config.redis.port, config.redis.host, { db: redisDB, auth_pass: config.redis.auth })
    return repository
}

function displayRepositoryInfo() {
    return repository.infoAsync()
		.then(response => {
	            var parts = response.split('\r\n')
            	    var version
            	    var versionString
            	    for (var i = 0; i < parts.length; i++){
                        if (parts[i].indexOf(':') !== -1){
                            var valParts = parts[i].split(':')
                            if (valParts[0] === 'redis_version'){
                                versionString = valParts[1]
                                version = parseFloat(versionString)
                                break
                            }
                        }
                    }
            	    if (!version){
                    log('error', logSystem, 'Could not detect redis version - must be super old or broken')
                	return
                    }
            	    else if (version < 2.6){
                	log('error', logSystem, "You're using redis version %s the minimum required version is 2.6. Follow the damn usage instructions...", [versionString])
                	return
            	    }
		    else {
			log('info', logSystem, "You're using redis version %s", [versionString])
			return
		    }
		})
		.catch(error => {
		    log('error', logSystem, 'Redis version check failed')
	            return
		})
}


function recordWorker(coin, miner) {
        repository.sadd(`${coin}:workers_ip:${miner.login}`, miner.ip)
	repository.hincrby(`${coin}:ports:${miner.port}`, 'users', 1)
	repository.hincrbyAsync(`${coin}:active_connections`, `${miner.login}~${miner.workerName}`, 1)
		.then(connectedWorkers => {return connectedWorkers})
		.catch(error => {})
}

function removeWorker(coin, miner) {
	repository.hincrby(`${coin}:ports:${miner.port}`, 'users', -1)
	repository.hincrbyAsync(`${coin}:active_connections`, `${miner.login}~${miner.workerName}`, -1)
		.then(connectedWorkers => {return connectedWorkers})
		.catch(error => {})
}

function recordShareData(coin, job, slushMining, dateNow,  miner, cleanupInterval, blockCandidate) {
    let dateNowSeconds = dateNow / 1000 | 0
    let updateScore
    if (slushMining) {
        updateScore = ['eval', `
            local age = (ARGV[3] - redis.call('hget', KEYS[2], 'lastBlockFound')) / 1000
            local score = string.format('%.17g', ARGV[2] * math.exp(age / ARGV[4]))
            redis.call('hincrbyfloat', KEYS[1], ARGV[1], score)
            return {score, tostring(age)}
            `,
            2 /*keys*/, config.coin + ':scores:roundCurrent', config.coin + ':stats',
            /* args */ miner.login, job.difficulty, Date.now(), config.poolServer.slushMining.weight]
    }
    else {
        job.score = job.difficulty
        updateScore = ['hincrbyfloat', config.coin + ':scores:roundCurrent', miner.login, job.score]
    }

    let redisCommands = [
        updateScore,
        ['hincrby', config.coin + ':shares_actual:roundCurrent', miner.login, job.difficulty],
        ['zadd', config.coin + ':hashrate', dateNowSeconds, [job.difficulty, miner.login, dateNow].join(':')],
        ['hincrby', config.coin + ':workers:' + miner.login, 'hashes', job.difficulty],
        ['hset', config.coin + ':workers:' + miner.login, 'lastShare', dateNowSeconds],
        ['expire', config.coin + ':workers:' + miner.login, (86400 * cleanupInterval)],
        ['expire', config.coin + ':payments:' + miner.login, (86400 * cleanupInterval)]
    ];

    if (miner.workerName) {
        redisCommands.push(['zadd', config.coin + ':hashrate', dateNowSeconds, [job.difficulty, miner.login + '~' + miner.workerName, dateNow].join(':')]);
        redisCommands.push(['hincrby', config.coin + ':unique_workers:' + miner.login + '~' + miner.workerName, 'hashes', job.difficulty]);
        redisCommands.push(['hset', config.coin + ':unique_workers:' + miner.login + '~' + miner.workerName, 'lastShare', dateNowSeconds]);
        redisCommands.push(['expire', config.coin + ':unique_workers:' + miner.login + '~' + miner.workerName, (86400 * cleanupInterval)]);
    }

    if (blockCandidate){
        redisCommands.push(['hset', config.coin + ':stats', 'lastBlockFound', Date.now()]);
        redisCommands.push(['rename', config.coin + ':scores:roundCurrent', config.coin + ':scores:round' + job.height]);
        redisCommands.push(['rename', config.coin + ':shares_actual:roundCurrent', config.coin + ':shares_actual:round' + job.height]);
        redisCommands.push(['hgetall', config.coin + ':scores:round' + job.height]);
        redisCommands.push(['hgetall', config.coin + ':shares_actual:round' + job.height]);
    }
    return repository.multi(redisCommands).execAsync()
}

function recordBlockCandidate(coin, height, hashHex, dateNowSeconds, difficulty, totalShares, totalScore) {
    repository.zaddAsync(`${coin}:blocks:candidates`, height, [hashHex, dateNowSeconds, difficulty, totalShares, totalScore].join(':'))
	.then(response => {})
	.catch(error => {
	    log('error', logSystem, 'Failed inserting block candidates %s \n %j', [hashHex, error])
	})
}

function getTelegramId(coin, miner) {
    return repository.hgetAsync(`${coin}:telegram`, miner)
}

function delTelegramId(coin, miner) {
    return repository.hdelAsync(`${coin}:telegram`, miner)
}

function getDefaultTelegramId(coin, chatId) {
    return repository.hgetAsync(`${coin}:telegram:default`, chatId)
}

function setDefaultTelegramId(coin, chatId, address) {
    return repository.hsetAsync(`${coin}:telegram:default`, chatId, address)
}

function setTelegramId(coin, address, chatId) {
    return repository.hsetAsync(`${coin}:telegram`, address, chatId)
}

function getTelegramBlockData(coin) {
    return repository.hgetallAsync(`${coin}:telegram:blocks`)
}

function getTelegramBlocksByChatId(coin, chatId) {
    return repository.hgetAsync(`${coin}:telegram:blocks`, chatId)
}

function delTelegramBlocksByChatId(coin, chatId) {
    return repository.hdelAsync(`${coin}:telegram:blocks`, chatId)
}

function setTelegramBlocksByChatId(coin, chatId, value) {
    return repository.hsetAsync(`${coin}:telegram:blocks`, chatId, value)
}

function getAllEmails(coin, miner) {
    return repository.hgetallAsync(`${coin}:notifications`, miner)
}

function getMinerEmail(coin, miner) {
    return repository.hgetAsync(`${coin}:notifications`, miner)
}

function getWorkersEligibleForPayment(coin, paymentOptions) {
    return repository.keysAsync(coin + ':workers:*')
        .then(keys => {
            let commands = keys.map(key => {
                return ['hmget', key, 'balance', 'minPayoutLevel']
            })
            return [commands, keys]
        })
        .then(commandsAndKeys => {
           return repository.multi(commandsAndKeys[0]).execAsync()
                .then(replies => {
                    let payments = {}
                    for (let i = 0; i < replies.length; i++){
			if (!replies[0]) continue
                        let parts = commandsAndKeys[1][i].split(':')
                        let workerId = parts[parts.length - 1]
                        let balance = parseInt(replies[0][i]) || 0

                        let minLevel = paymentOptions.minPayment
                        let maxLevel = paymentOptions.maxPayment
                        let defaultLevel = minLevel

			let payoutLevel = minLevel
			if (replies[1])
                            payoutLevel = parseInt(replies[1][i]) || minLevel
                        if (payoutLevel < minLevel) payoutLevel = minLevel
                        if (maxLevel && payoutLevel > maxLevel) payoutLevel = maxLevel
                        minPayoutLevel = payoutLevel

                        if (balance >= minPayoutLevel) {
                            let remainder = balance % paymentOptions.denomination
                            let payout = balance - remainder
                            if (paymentOptions.dynamicTransferFee && paymentOptions.minerPayFee) {
                                payout -= paymentOptions.transferFee
                            }
                            payments[workerId] = payout
                        }

                    }
                    return payments
                 })
                 .catch(error => {
                     console.log('error2 ', error)
                     //log('error', logSystem, 'Error with getting balances from redis %j', [error])
                 })
        })
        .catch(error => {
            console.log('error1 ', error)
        })
}

function getPoolStats(coin, apiOptions) {
    let redisCommands = [
        ['zremrangebyscore', `${coin}:hashrate`, '-inf', ''],
        ['zrange', `${coin}:hashrate`, 0, -1],
        ['hgetall', `${coin}:stats`],
        ['zrange', `${coin}:blocks:candidates`, 0, -1, 'WITHSCORES'],
        ['zrevrange', `${coin}:blocks:matured`, 0, apiOptions.blocks - 1, 'WITHSCORES'],
        ['hgetall', `${coin}:scores:roundCurrent`],
        ['hgetall', `${coin}:stats`],
        ['zcard', `${coin}:blocks:matured`],
        ['zrevrange', `${coin}:payments:all`, 0, apiOptions.payments - 1, 'WITHSCORES'],
        ['zcard', `${coin}:payments:all`],
        ['keys', `${coin}:payments:*`],
        ['hgetall', `${coin}:shares_actual:roundCurrent`],
    ]
    let now = Date.now()
    let windowTime = (((now / 1000) - apiOptions.hashrateWindow) | 0).toString()
    redisCommands[0][3] = '(' + windowTime

    return repository.multi(redisCommands).execAsync()
}

function getMaturedBlocks(coin, height, blocks) {
    if (height && blocks)
    	return repository.zrevrangeAsync(`${coin}:blocks:matured`, `(${height}`, '-inf', 'WITHSCORES', 'LIMIT', 0, blocks)
    else
    	return repository.zrevrangeAsync(`${coin}:blocks:matured`, 0, -1, 'WITHSCORES')
}

function getMinerStats(coin, address, apiConfig) {
    let redisCommands = [
        ['hgetall', `${coin}:workers:${address}`],
        ['zrevrange', `${coin}:payments:${address}`, 0, apiConfig.payments - 1, 'WITHSCORES'],
        ['keys', `${coin}:unique_workers:${address}~*`],
        ['get', `${coin}:charts:hashrate:${address}`]
    ]
    return repository.multi(redisCommands).execAsync()
}

function getMinerExists(coin, address) {
    return repository.existsAsync(`${coin}:workers:${address}`)
}

function getMinerChartData(coin, address, workers) {
    let redisCommands = [];
    for (let i in workers){
        redisCommands.push(['hgetall', `${coin}:unique_workers:${address}~${workers[i].name}`])
        redisCommands.push(['get', `${coin}:charts:worker_hashrate:${address}~${workers[i].name}`])
    }
    return repository.multi(redisCommands).execAsync()
}

function getPayments(coin, apiConfig, time, address) {
    let key = `${coin}:payments:${address?address:'all'}`
    return repository.zrevrangebyscoreAsync(key, `(${time}`, '-inf', 'WITHSCORES', 'LIMIT', 0, apiConfig.payments)
}

function getTopMiners(coin) {
    return repository.keysAsync(coin + ':workers:*')
        .then(keys => {
            let commands = keys.map(key => {
                return ['hmget', key, 'lastShare', 'hashes']
            })
            return [commands, keys]
        })
        .then(commandsAndKeys => {
            return repository.multi(commandsAndKeys[0]).execAsync()
	        .then(redisData => {
		    return [commandsAndKeys[1], redisData]
		})
        })
        .catch(error => {
            console.log('error1 ', error)
        })
}

function getMinerMinPayout(coin, address) {
    return repository.hgetAsync(`${coin}:workers:${address}`, 'minPayoutLevel')
}

function setMinerMinPayout(coin, address, payoutLevel) {
    return repository.hsetAsync(`${coin}:workers:${address}`, 'minPayoutLevel', payoutLevel)
}

function setMinerEmailAddress(coin, address, email) {
   return repository.hsetAsync(`${coin}:notifications`, address, email)
}

function delMinerEmailAddress(coin, address) {
   return repository.hdelAsync(`${coin}:notifications`, address)
}

function getStatsAdmin(coin) {
    let redisCommands = [
        ['keys', `${coin}:workers:*`],
        ['zrange', `${coin}:blocks:matured`, 0, -1],
    ]
    return repository.multi(redisCommands).execAsync()
	       .then(replies => {
                   let commands = replies[0].map(key => {
                           return ['hmget', key, 'balance', 'paid']
                       })
                   return [commands, replies[1]]
	       })
	      .then(commandsAndBlocks => {
		  return repository.multi(commandsAndBlocks[0]).execAsync()
			    .then(replies => {
				return [replies, commandsAndBlocks[1]]
			    })
	      })
	      .catch(error => {
	      })
}

function getUserStatsAdmin(coin) {
    return repository.keysAsync(coin + ':workers:*')
        .then(keys => {
            let commands = keys.map(key => {
                return ['hmget', key, 'balance', 'paid', 'lastShare',  'hashes']
            })
            return [commands, keys]
        })
	.then(commandsAndKeys => {
	    return repository.multi(commandsAndKeys[0]).execAsync()
			.then(replies => {
			    return [commandsAndKeys[1], replies]
			})
	})
}

function getUserPortsAdmin(coin) {
    return repository.keysAsync(coin + ':ports:*')
        .then(keys => {
            let commands = keys.map(key => {
                return ['hmget', key, 'port', 'users']
            })
            return [commands, keys]
        })
	.then(commandsAndKeys => {
	    return repository.multi(commandsAndKeys[0]).execAsync()
			.then(replies => {
			    return [commandsAndKeys[1], replies]
			})
	})
}

function setMonitoringStatus(coin, module, stats) {
    let redisCommands = []
    for (let property in stats) {
        redisCommands.push(['hset', `${coin}:status:${module}`, property, stats[property]])
    }
    return repository.multi(redisCommands).execAsync()
}

function getMonitoringStatus(coin, modules) {
    let redisCommands = modules.map(key => {
				    return ['hgetall', `${coin}:status:${key}`]
				})
    return repository.multi(redisCommands).execAsync()
}

function findMinerIPByAddress(coin, address, ip) {
    return repository.sismemberAsync([`${coin}:workers_ip:${address}`, ip])
}

module.exports = {
    init : init,
    displayRepositoryInfo : displayRepositoryInfo,
    recordWorker : recordWorker,
    removeWorker : removeWorker,
    recordShareData : recordShareData,
    recordBlockCandidate : recordBlockCandidate,
    getTelegramId : getTelegramId,
    setTelegramId : setTelegramId,
    delTelegramId : delTelegramId,
    getTelegramBlockData : getTelegramBlockData,
    getAllEmails : getAllEmails,
    getMinerEmail : getMinerEmail,
    getWorkersEligibleForPayment : getWorkersEligibleForPayment,
    getPoolStats : getPoolStats,
    getMaturedBlocks : getMaturedBlocks,
    getMinerStats : getMinerStats,
    getMinerChartData : getMinerChartData,
    getMinerExists : getMinerExists,
    getPayments : getPayments,
    getTopMiners : getTopMiners,
    getMinerMinPayout : getMinerMinPayout,
    setMinerMinPayout : setMinerMinPayout,
    setMinerEmailAddress : setMinerEmailAddress,
    delMinerEmailAddress : delMinerEmailAddress,
    getDefaultTelegramId : getDefaultTelegramId,
    setDefaultTelegramId : setDefaultTelegramId,
    getTelegramBlocksByChatId : getTelegramBlocksByChatId,
    setTelegramBlocksByChatId : setTelegramBlocksByChatId,
    delTelegramBlocksByChatId : delTelegramBlocksByChatId,
    getStatsAdmin : getStatsAdmin,
    getUserStatsAdmin : getUserStatsAdmin,
    getUserPortsAdmin : getUserPortsAdmin,
    setMonitoringStatus : setMonitoringStatus,
    getMonitoringStatus : getMonitoringStatus,
    findMinerIPByAddress : findMinerIPByAddress
}
