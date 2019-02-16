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
        repository.sadd(`${coin}:worker_ip:${miner.login}`, miner.ip)
	repository.hincrby(`${coin}:ports:${miner.port}`, 'users', 1)
	repository.hincrbyAsync(`${coin}:active_connections`, `${miner.login}~${miner.workerName}`, 1)
		.then(connectedWorkers => {return connectedWorkers})
		.catch(error => {})
}

function removeWorker(coin, miner) {
	repository.hincrby(`${coin}:ports:${miner.port}`, 'users', -1)
	respository.hincrbyAsync(`${coin}:active_connections`, `${miner.login}~${miner.workerName}`, -1)
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
    repository.multi(redisCommands).execAsync()
	.then(replies => {
	    return replies
	})
	.catch(error => {
	    log('error', logSystem, 'Failed to insert share datat into redis %j \n %j', [error, redisCommands])
	    return null
	})
}

function recordBlockCandidate(coin, height, hashhex, dateNowSeconds, difficulty, totalShares, totalScore) {
    repository.zaddAsync(`${coin}:blocks:candidates`, height, [hashhex, dateNowSeconds, difficulty, totalShares, totalScore].join(':'))
	.then(response => {})
	.catch(error => {
	    log('error', logSystem, 'Failed inserting block candidates %s \n %j', [hashHex, error])
	})
}

function getTelegramId(coin, miner) {
    repository.hgetAsync(`${coin}:telegram`, miner)
	.then(chatId => {
	    return chatId
	})
	.catch(error => {
	    return null
	})
}

function getTelegramBlockData(coin) {
    repository.hgetallAsync(`${coin}:telegram:blocks`)
	.then(data => {
	    return data
	})
	.catch(error => {
	    return null
	})
}

function getAllEmails(coin, miner) {
    repository.hgetallAsync(`${coin}:notifications`, miner)
	.then(data => {
	    return data
	})
	.catch(error => {
	    return null
	})
}

function getMinerEmail(coin, miner) {
    repository.hgetAsync(`${coin}:notifications`, miner)
	.then(email => {
	    return email
	})
	.catch(error => {
	    return null
	})
}


module.exports = {
    init : init,
    displayRepositoryInfo : displayRepositoryInfo,
    recordWorker : recordWorker,
    removeWorker : removeWorker,
    recordShareData : recordShareData,
    recordBlockCandidate : recordBlockCandidate,
    getTelegramId : getTelegramId,
    getTelegramBlockData : getTelegramBlockData,
    getAllEmails : getAllEmails,
    getMinerEmail : getMinerEmail
}
