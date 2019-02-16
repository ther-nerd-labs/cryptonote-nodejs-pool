/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Handle communications to APIs
 **/

// Load required modules
const http = require('http');
const https = require('https');
const request_daemon = require('request-promise-native');
const request_wallet = require('request-promise-native');

const queue = require('promise-queue');

const queue_daemon = new queue(1, 5)
const queue_wallet = new queue(1, 5)

const agent_daemon = new http.Agent({'keepAlive': true, 'maxSockets': 1})
const agent_wallet = new http.Agent({'keepAlive': true, 'maxSockets': 1})


/**
 * Send API request using JSON HTTP
 **/
async function jsonHttpRequest(host, port, data, callback, path){
    try {
        path = path || '/json_rpc';
        callback = callback || function(){}; 
        let options = {
            'uri': `${(port === 443 ? 'https' : 'http')}://${host}:${port}${path}`,
            'method': data ? 'POST' : 'GET',
            'headers' : { 
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                     }
        }
        options['json'] = data
let response =await request_wallet(options)

callback(null, response)
/*
queue_daemon.add(() => {
      return request_daemon(options)
            .then(response => {
                callback(null, response)
            })
            .catch(error => {
console.log(options)
                callback(error, {})
            })
})
*/
    } catch(error){
//        console.log('catch ' , error)
        callback(error, {})
    }
}

async function jsonHttpRequest_wallet(host, port, data, callback, path){
    try {
        path = path || '/json_rpc';
        callback = callback || function(){}; 
        let options = {
            'uri': `${(port === 443 ? 'https' : 'http')}://${host}:${port}${path}`,
            'method': data ? 'POST' : 'GET',
            'headers' : { 
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                     }
        }
        options['json'] = data
//queue_wallet.add(() => {

let response =await request_wallet(options)

callback(null, response)


/*
       return request_wallet(options)
            .then(response => {
                callback(null, response)
            })
            .catch(error => {
console.log(options)
                callback(error, {})
            })
*/
//})
    } catch(error){
       console.log('catch ' , error)
        callback(error, {})
    }
}

/**
 * Send RPC request
 **/


function daemonRpc(host, port, method, params, callback, username, password){
    let payload = {
        id: "0",
        jsonrpc: "2.0",
        method: method,
	params: params
    };
//    if (Object.keys(params).length !== 0)
//	payload['params'] = params
/*    if (password !== undefined) {
        if (username !== undefined){
            payload['auth'] = {'user': username, 'pass': password, 'sendImmediately':false}
            payload['agent'] = agent_daemon
            payload['forever'] = true
        } else {
            payload['pass'] = password;
        }
    }
*/
    //let data = JSON.stringify(payload);
    jsonHttpRequest(host, port, payload, function(error, replyJson){
        if (error){
            callback(error, {});
            return;
        }
        callback(replyJson.error, replyJson.result)
    });
}

function walletRpc(host, port, method, params={}, callback, username, password){
    let payload = {
        id: "0",
        jsonrpc: "2.0",
        method: method,
	json: true
    };
    if (Object.keys(params).length !== 0)
	payload['params'] = params
/*    if (password !== undefined) {
        if (username !== undefined){
            payload['auth'] = {'user': username, 'pass': password, 'sendImmediately':false}
            payload['agent'] = agent_wallet
            payload['forever'] = true
        } else {
            payload['password'] = password;
        }
    }
*/
    //let data = JSON.stringify(payload);
    jsonHttpRequest_wallet(host, port, payload, function(error, replyJson){
        if (error){
            callback(error, {});
            return;
        }
        callback(replyJson.error, replyJson.result)
    });

    //jsonHttpRequest_wallet(host, port, payload, callback)
}


/**
 * Send RPC requests in batch mode
 **/
function batchRpc(host, port, array, callback){
    let rpcArray = [];
    for (let i = 0; i < array.length; i++){
        rpcArray.push({
            id: i.toString(),
            jsonrpc: "2.0",
            method: array[i][0],
            params: array[i][1]
        });
    }
    let data = JSON.stringify(rpcArray);
    jsonHttpRequest(host, port, data, callback);
}

/**
 * Send RPC request to pool API
 **/
function poolRpc(host, port, path, callback){
    jsonHttpRequest(host, port, '', callback, path);
}

/**
 * Exports API interfaces functions
 **/
module.exports = function(daemonConfig, walletConfig, poolApiConfig){
    return {
        batchRpcDaemon: function(batchArray, callback){
            batchRpc(daemonConfig.host, daemonConfig.port, batchArray, callback);
        },
        rpcDaemon: function(method, params, callback){
            daemonRpc(daemonConfig.host, daemonConfig.port,
		method, params, callback,
		);
        },
        rpcWallet: function(method, params, callback){
            walletRpc(walletConfig.host, walletConfig.port, 
                method, params, callback,
		);
        },
        pool: function(path, callback){
            let bindIp = config.api.bindIp ? config.api.bindIp : "0.0.0.0";
            let poolApi = (bindIp !== "0.0.0.0" ? poolApiConfig.bindIp : "127.0.0.1");
            poolRpc(poolApi, poolApiConfig.port, path, callback);
        },
        jsonHttpRequest: jsonHttpRequest
    }
};
