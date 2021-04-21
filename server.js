/** Copyright © 2020 The Things Industries B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file server.js
 *
 * @copyright Copyright (c) 2020 The Things Industries B.V.
 *
 */

/**
 * ------------- WARNING -------------
 * This FUOTA test server should only be used for prototyping and testing.
 * This server should not be used in production and TTI is not responsible for the usage of this demo server in deployed environments.
 * This server is sent to Oxit and Invue for the purposes of support and testing and should not be shared with other entities.
 */

// Start of configuration area
// test-app settings
var HOST = 'eu1.cloud.thethings.industries';
var APP_ID = 'security-dev-eng-test@invue';
var MULTICAST_APP_ID = 'security-dev-eng-test'; //ray-was test-app, to security-dev-eng-test@invue
var MULTICAST_DEV_ID = 'multicast-fuota-security';//ray-set to m1 was mc4
var APP_KEY = 'NNSXS.AVYHMVFZEBMLDHTRLJWB27BMRSB6LOH6BNTG5PA.U3LBFW4CGBL6MZLOP3GBW7OAPHNZN5TSEQ2GBTAT3PZX4NVNKIMA';

/*
// security app settings
var HOST = 'eu1.cloud.thethings.industries';
var APP_ID = 'invue-live-dev-secpoc@invue';
var MULTICAST_APP_ID = 'invue-live-dev-secpoc';
var MULTICAST_DEV_ID = 'multicast-28e3f438';
var APP_KEY = 'NNSXS.MMMWIX3DPIGD5N6JFYKFM5F7TLJY5EHT2K2SR4A.BZDXD636VZZJTFLVYADQYZIK4KZCZ3MMRJZ635455TGSWVCYRC2Q';
*/

//var GATEWAY_ID = '9181003t19c00581';
var GATEWAY_ID = '9181000t20500258-2';


//all devices that you want to update
//@oxit, adjust here
const devices = [
	'CC1FC40003000000',
];


var PORT = 8883;
var TENANT_NAME = 'invue'; //for TTI cloud hosted only
var extra_info = 0; // set to 1 to get more debug info
//-----------------------------------------------------------//
var options = {
    username: APP_ID,
    password: APP_KEY,
    port: PORT,
    host: HOST,
    //key: KEY,
    //cert: CERT,
    rejectUnauthorized: false,
    protocol: 'mqtts'
}
const PACKET_FILE = process.argv[2];
const DATARATE = process.env.LORA_DR || 12; //@oxit, adjust here
if (!PACKET_FILE) throw 'Syntax: loraserver.io PACKET_FILE'
const mqtt = require('mqtt');
const client = mqtt.connect(options);
const gpsTime = require('gps-time');
const fs = require('fs');
const stats = fs.statSync(PACKET_FILE);
const fileSize = stats["size"];
const maxPayloadSize = 222;
const fragSize = maxPayloadSize - 3;
const path = require('path');
const rp = require('request-promise');
const crc = require('node-crc');
const fileData = fs.readFileSync(PACKET_FILE, null);
const fileCrc64 = crc.crc64jones(fileData).toString('hex');
const PARITY_FRAMES = 100;
const FRAG_TX_DELAY = 2000;
const PARITY_FRAG_TX_DELAY = 5000;
const CLASS_C_WAIT_S = 60;
const CLASS_C_LONG_WAIT_S = 180; // 180 seconds
//const CLASS_C_LONG_WAIT_S = 86400; // 24 hours

console.log("fileCrc64 = ", fileCrc64);
let crc64Buffer = Buffer.alloc(8, 0);
for( j = 0; j < 8; j++ ) {
    crc64Buffer[j] = parseInt( fileCrc64[j * 2] + fileCrc64[( j * 2 ) + 1], 16 );
}
console.log( "CRC64 buffer: ", crc64Buffer );

// details for the multicast group
const mcDetails = {
    application_id: MULTICAST_APP_ID,
    device_id: MULTICAST_DEV_ID,
};

let classCStarted = false;
let fragCount = 0;
let deviceMap = devices.reduce((curr, eui) => {
    curr[eui] = {
        clockSynced: false,
        fragSessionAns: false,
        mcSetupAns: false,
        mcStartAns: false,
        applicationID: null,
        msgWaiting: null,
        fuotaDoneSuccessfully: false
    };
    return curr;
}, {});

let startTime = null;

client.on('error', err => console.error('Error on MQTT subscriber', err));
client.on('connect', function () {
    console.log('MQTT client connected!');

    // Set the max wait time for all devices to successfully check-in before sending mc class C packets
    if (!startTime) {
       let serverTime = gpsTime.toGPSMS(Date.now()) / 1000 | 0;
       startTime = serverTime + CLASS_C_LONG_WAIT_S; // 60 seconds from now

       setTimeout(() => {
           try {
               startSendingClassCPacketsForActiveDevices();
           } catch (e) {
               console.error(e);
           }
       }, (CLASS_C_LONG_WAIT_S + 10) * 1000); // because the delta drift that we don't know (see above)

       startCountdownToStartClassCSession(CLASS_C_LONG_WAIT_S);
   }

    client.subscribe('#', function (err) { //subscribed to all topics
        if (err) {
            return console.error('Failed to subscribe', err);
        }
        console.log('Subscribed to all application events');
/* 		devices.forEach(eui => {
			sendForceDeviceResyncReq(eui);
		});
 */
    });
});

client.on('message', async function (topic, message) {
    try {
        if (extra_info)
            console.log('msg', message.toString('utf-8'));
        // only interested in uplink messages
        if (!/\/up$/.test(topic)) return;

        // message is Buffer
        let m = JSON.parse(message.toString('utf-8'));

        // device that we don't care about
        if (!deviceMap[m.end_device_ids.dev_eui]) {
            console.log(m.end_device_ids.dev_eui);
            console.log(deviceMap);
            console.log("Unhandled device EUI: " + m.end_device_ids.dev_eui);
            return;
        }

        if (deviceMap[m.end_device_ids.dev_eui].clockSynced === false) {
            console.log('Send force device resync request');
            //sendForceDeviceResyncReq(m.end_device_ids.dev_eui);
            console.log('here 1');
            //setTimeout(sendForceDeviceResyncReq(m.end_device_ids.dev_eui), 1000);
            setTimeout(function () {
               try {
                   sendForceDeviceResyncReq(m.end_device_ids.dev_eui)
               } catch (e) {
                 console.error(e);
               }
             }, 1000);
        }

        if (m.uplink_message.f_port === 202 /* clock sync */ ) {
            let body = Buffer.from(m.uplink_message.frm_payload, 'base64');
            if (body[0] === 0x1 /* CLOCK_APP_TIME_REQ */ ) {
                let deviceTime = body[1] + (body[2] << 8) + (body[3] << 16) + (body[4] << 24);
                let serverTime = gpsTime.toGPSMS(Date.now()) / 1000 | 0;
                console.log('deviceTime', deviceTime, 'serverTime', serverTime);

                let adjust = serverTime - deviceTime | 0;
                let resp = [1, adjust & 0xff, (adjust >> 8) & 0xff, (adjust >> 16) & 0xff, (adjust >> 24) & 0xff, 0b0000 /* tokenAns */ ];
                let responseMessage = {
                    "downlinks": [{
                        "priority": "NORMAL",
                        "f_port": 202,
                        "frm_payload": Buffer.from(resp).toString('base64')
                    }]
                };

                deviceMap[m.end_device_ids.dev_eui].msgWaiting = responseMessage;

                deviceMap[m.end_device_ids.dev_eui].clockSynced = true;
                deviceMap[m.end_device_ids.dev_eui].applicationID = m.end_device_ids.application_ids.application_id;

                console.log('Clock sync for device', m.end_device_ids.dev_eui, adjust, 'seconds');

                //if (devices.some(eui => deviceMap[eui].clockSynced)) {
                if (deviceMap[m.end_device_ids.dev_eui].clockSynced) {
                    console.log('The device %s has had their clocks synced, setting up mc group...', m.end_device_ids.dev_eui);
                    setTimeout(function () {
                        try {
                            sendMcGroupSetup(m.end_device_ids.dev_eui)
                        } catch (e) {
                            console.error(e);
                        }
                    }, 1000);
                }
            } else {
                console.warn('Could not handle clock sync request', body);
            }
        }
        if (m.uplink_message.f_port === 200 /* mc group cmnds */ ) {
            let body = Buffer.from(m.uplink_message.frm_payload, 'base64');
            if (body[0] === 0x2) { // McGroupSetupAns
                if (body[1] === 0x0) {
                    deviceMap[m.end_device_ids.dev_eui].mcSetupAns = true;
                } else {
                    console.warn('Unexpected answer for McGroupSetupAns from', m.end_device_ids.dev_eui, body)
                }

                //if (devices.every(eui => deviceMap[eui].mcSetupAns)) {
                if (deviceMap[m.end_device_ids.dev_eui].mcSetupAns) {
                    console.log('The device %s has received multicast group, setting up fragsession...', m.end_device_ids.dev_eui);
                    setTimeout(function () {
                        try {
                            sendFragSessionSetup(m.end_device_ids.dev_eui)
                        } catch (e) {
                            console.error(e);
                        }
                    }, 1000);
                }
            } else if (body[0] === 0x4) { // McClassCSessionAns
                if (body[1] !== 0x0) return console.warn('Unexpected byte[1] for McClassCSessionAns', m.end_device_ids.dev_eui, body);

                let tts = body[2] + (body[3] << 8) + (body[4] << 16);
                console.log(m.end_device_ids.dev_eui, 'time to start', tts, 'startTime is', startTime, 'currtime is', gpsTime.toGPSMS(Date.now()) / 1000 | 0);

                deviceMap[m.end_device_ids.dev_eui].mcStartAns = true;

                // so this app cannot properly check the delta, as we don't know when the network is gonna send
                // should be calculated at that very moment, so now there can be a few seconds delay
                let delta = (gpsTime.toGPSMS(Date.now()) / 1000 | 0) + tts - startTime;
                if (Math.abs(delta) > 6) {
                    console.log('Delta is too big for', m.end_device_ids.dev_eui, Math.abs(delta));
                } else {
                    console.log('Delta is OK', m.end_device_ids.dev_eui, delta);
                }
            } else {
                console.warn('Could not handle Mc Group command', body);
            }
        }
        if (m.uplink_message.f_port === 201 /* frag session */ ) {
            let body = Buffer.from(m.uplink_message.frm_payload, 'base64');
            if (body[0] === 0x2) { // FragSessionSetupAns
                if (body[1] === 0x0) {
                    deviceMap[m.end_device_ids.dev_eui].fragSessionAns = true;
                } else {
                    console.warn('Unexpected answer for FragSessionSetupAns from', m.end_device_ids.dev_eui, body)
                }

                //if (devices.every(eui => deviceMap[eui].fragSessionAns)) {
                if (deviceMap[m.end_device_ids.dev_eui].fragSessionAns) {
                    console.log('The device %s has received frag session, sending mc start msg...', m.end_device_ids.dev_eui);
                    setTimeout(function () {
                        try {
                            sendMcClassCSessionReq(m.end_device_ids.dev_eui)
                        } catch (e) {
                            console.error(e);
                        }
                    }, 1000);
                }
            } else if (body[0] === 0x5) { // DATA_BLOCK_AUTH_REQ
                let hash = '';
                for (let ix = 5; ix > 1; ix--) {
                    hash += body.slice(ix, ix + 1).toString('hex');
                }
                console.log('Received DATA_BLOCK_AUTH_REQ', m.end_device_ids.dev_eui, hash);
            } else {
                console.warn('Could not handle Mc Group command', body);
            }
        }
        if (m.uplink_message.f_port === 101 /* success/failure report */ ) {
            let body = Buffer.from(m.uplink_message.frm_payload, 'base64');
            if (body[0] === 0x80) {
                let j = 0;
                if ((crc64Buffer[0] === body[8]) && (crc64Buffer[1] === body[7]) && (crc64Buffer[2] === body[6]) &&
                    (crc64Buffer[3] === body[5]) && (crc64Buffer[4] === body[4]) && (crc64Buffer[5] === body[3]) &&
                    (crc64Buffer[6] === body[2]) && (crc64Buffer[7] === body[1])) {
                    deviceMap[m.end_device_ids.dev_eui].fuotaDoneSuccessfully = true;
                } else {
                    deviceMap[m.end_device_ids.dev_eui].fuotaDoneSuccessfully = false;
                }
            }
        }
        if (deviceMap[m.end_device_ids.dev_eui].msgWaiting) {
            let msgWaiting = deviceMap[m.end_device_ids.dev_eui].msgWaiting;
            console.log("publishing as", m.end_device_ids.application_ids.application_id + '@' + TENANT_NAME, m.end_device_ids.device_id);
            client.publish(`v3/${m.end_device_ids.application_ids.application_id+'@'+TENANT_NAME}/devices/${m.end_device_ids.device_id}/down/push`, Buffer.from(JSON.stringify(msgWaiting), 'utf8'));
            deviceMap[m.end_device_ids.dev_eui].msgWaiting = null;
        }
    } catch (e) {
        console.log(e);
    }
});

function sendForceDeviceResyncReq(dev_eui) {
    // mcgroupsetup
    //@oxit, adjust here
    // let NbTransmissions = 3;
    let ForceDeviceResyncReq = {
        "downlinks": [{
            "priority": "NORMAL",
            "f_port": 202,
            "frm_payload": Buffer.from([0x03, 0x01]).toString('base64')
        }]
    };

    devices.forEach(eui => {
        let dm = deviceMap[eui];
        if (dm.clockSynced) return;

        dm.msgWaiting = ForceDeviceResyncReq;
    });

    // retry
    setTimeout(() => {
        try {
            if (deviceMap[dev_eui].clockSynced === false) {
                console.log('sendForceDeviceResyncReq ' + deviceMap[dev_eui]);
                sendForceDeviceResyncReq(dev_eui);
            }
        } catch (e) {
            console.error(e);
        }
    }, 20000);
}

function sendMcGroupSetup(dev_eui) {
    if (classCStarted) return;
    console.log('sendMcGroupSetup - ', dev_eui);
    // mcgroupsetup
    //@oxit, adjust here
    let mcGroupSetup = {
        "downlinks": [{
            "priority": "NORMAL",
            "f_port": 200,
            "frm_payload": Buffer.from([0x02, 0x00,
                0xFF, 0xFF, 0xFF, 0x01, // McAddr
                0x01, 0x5E, 0x85, 0xF4, 0xB9, 0x9D, 0xC0, 0xB9, 0x44, 0x06, 0x6C, 0xD0, 0x74, 0x98, 0x33, 0x0B, //McKey_encrypted
                0x0, 0x0, 0x0, 0x0, // minFCnt
                0xff, 0x0, 0x0, 0x0 // maxFCnt
            ]).toString('base64')
        }]
    };
     /**
     * [DBG ][LWUC]: handleMulticastSetupReq mcIx=0
[DBG ][LWUC]:   mcAddr:         0x01ffffff
[DBG ][LWUC]:   NwkSKey:
                 ff 70 1d 83 68 a4 c6 58 60 48 ff a2 9d 8a e0 10 
[DBG ][LWUC]:   AppSKey:
                 f7 d9 66 7a cd 8e b1 dd e3 80 75 1a 85 93 ea ec 
[DBG ][LWUC]:   minFcFCount:    0
[DBG ][LWUC]:   maxFcFCount:    255
     */
/*    devices.forEach(eui => {
        let dm = deviceMap[eui];
        if (dm.mcSetupAns) return;

        dm.msgWaiting = mcGroupSetup;
    });
*/

    let dm = deviceMap[dev_eui];
    if (dm.mcSetupAns) return;

    dm.msgWaiting = mcGroupSetup;

    // retry
    setTimeout(() => {
        try {
            //if (devices.some(eui => !deviceMap[eui].mcSetupAns)) {
            if (!deviceMap[dev_eui].mcSetupAns) {
                console.log('sendMcGroupSetup: ' + dev_eui);
                console.log('sendMcGroupSetup: ' + JSON.stringify(deviceMap[dev_eui]));
                sendMcGroupSetup(dev_eui);
            }
        } catch (e) {
            console.error(e);
        }
    }, 20000);
}

function sendFragSessionSetup(dev_eui) {
    if (classCStarted) return;
    console.log('sendFragSessionSetup - ', dev_eui);

    fragCount = Math.floor(fileSize / fragSize);
    if (fileSize % fragSize)
        fragCount++;

    let padding = fragSize - (fileSize % fragSize);
    let cmdBuffer = Buffer.alloc(11, 0); //[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

    /*   DBG   */
    // fragCount -= 3;

    cmdBuffer[0] = 0x02; // Command ID
    cmdBuffer[1] = 0x00; // Frag session
    cmdBuffer[2] = fragCount & 0x00FF;
    cmdBuffer[3] = (fragCount >> 8) & 0x00FF;
    cmdBuffer[4] = fragSize;
    cmdBuffer[5] = 0x00; // Control
    cmdBuffer[6] = padding;
    cmdBuffer[7] = 0x00; // Descriptor[0]
    cmdBuffer[8] = 0x00; // Descriptor[1]
    cmdBuffer[9] = 0x00; // Descriptor[2]
    cmdBuffer[10] = 0x00; // Descriptor[3]

    let msg = {
        "downlinks": [{
            "priority": "NORMAL",
            "f_port": 201,
            //"frm_payload": Buffer.from(parsePackets()[0]).toString('base64')
            "frm_payload": Buffer.from(cmdBuffer).toString('base64')
        }]
    };

    let dm = deviceMap[dev_eui];
    if (dm.fragSessionAns) return;

    dm.msgWaiting = msg;

/*     devices.forEach(eui => {
        let dm = deviceMap[eui];
        if (dm.fragSessionAns) return;

        dm.msgWaiting = msg;
    });
 */
    // retry
    setTimeout(() => {
        try {
            //if (devices.some(eui => !deviceMap[eui].fragSessionAns)) {
            if (!deviceMap[dev_eui].fragSessionAns) {
                console.log('sendFragSessionSetup ' + JSON.stringify(deviceMap));
                console.log('sendFragSessionSetup ' + dev_eui);
                sendFragSessionSetup(dev_eui);
            }
        } catch (e) {
            console.error(e);
        }
    }, 20000);
}

function sendMcClassCSessionReq(dev_eui) {
    if (classCStarted) return;

    console.log('sendMcClassCSessionReq - ', dev_eui);

    if (!startTime) {
        let serverTime = gpsTime.toGPSMS(Date.now()) / 1000 | 0;
        startTime = serverTime + CLASS_C_WAIT_S; // 60 seconds from now

        setTimeout(() => {
            try {
                startSendingClassCPackets();
            } catch (e) {
                console.error(e);
            }
        }, (CLASS_C_WAIT_S + 10) * 1000); // because the delta drift that we don't know (see above)
    }

    let msg = {
        "downlinks": [{
            "priority": "NORMAL",
            "f_port": 200,
            "frm_payload": Buffer.from([
                0x4,
                0x0, // mcgroupidheader
                startTime & 0xff, (startTime >> 8) & 0xff, (startTime >> 16) & 0xff, (startTime >> 24) & 0xff,
                0x0C, // session timeout
                0x68, 0xE2, 0x8C, // dlfreq 923300000
                //0xD8, 0xF9, 0x8C, // dlfreq 923900000
                DATARATE // dr
            ]).toString('base64')
        }]
    };
/*     devices.forEach(eui => {
        let dm = deviceMap[eui];
        if (dm.mcStartAns) return;

        dm.msgWaiting = msg;
    });
 */
    let dm = deviceMap[dev_eui];
    if (dm.mcStartAns) return;

    dm.msgWaiting = msg;

    // retry
    setTimeout(() => {
        //if (devices.some(eui => !deviceMap[eui].mcStartAns)) {
        if (!deviceMap[dev_eui].mcStartAns) {
            console.log('devices 4' + JSON.stringify(deviceMap));
            sendMcClassCSessionReq(dev_eui);
        }
    }, 20000);
}

function sleep(ms) {
    return new Promise((res, rej) => setTimeout(res, ms));
}

function prbs23(start) {
    let x = start;
    let b0, b1;

    b0 = x & 1;
    b1 = (x & 32) / 32;
    x = (x / 2) + (b0 ^ b1) * (1 << 22);

    return Math.floor(x);
}

function matrix_line(N, M) {
    let m;
    let Mpwr, x, r, nb_coeff;
    let parityBuf = Buffer.alloc(M, 0);

    Mpwr = 1 << Math.floor(Math.log2(M)); // Calculate the power of 2 of log2( M )
    if (M === Mpwr) { // Check if M is power of 2
        m = 1;
    } else {
        m = 0;
    }

    x = 1 + 1001 * N;
    nb_coeff = 0;

    //    for (let i=0; i<M; i++) {
    //        parityBuf[i] = 0;
    //    }

    while (nb_coeff < (M >> 1)) {
        r = 1 << 16;
        while (r >= M) {
            x = prbs23(x);
            r = x % (M + m);
        }
        parityBuf[r] = 1;
        nb_coeff++;
    }

    return parityBuf;
}

function parsePackets() {
    let packets = [];
    let fileData = fs.readFileSync(PACKET_FILE, null);
    let pktIdx = 1;
    let fragSesIdx = 0;
    while (fileData.length > 0) {
        let buff = Buffer.alloc(fragSize + 3, 0)
        let tmp = pktIdx | ((fragSesIdx << 14) & 0xC000);

        buff[0] = 0x08;
        buff[1] = (tmp & 0x00FF);
        buff[2] = (tmp >> 8) & 0x00FF;

        for (let i = 0; i < (fragSize); i++) {
            buff[i + 3] = fileData.slice(0, fragSize)[i];
        }
        packets.push(buff)
        fileData = fileData.slice(fragSize)

        pktIdx++;
        /*             if(pktIdx >= 0x3FFF) {
                        pktIdx = 0;	// Rollover for 14-bits number
                        console.log("Warning: Packet Index Overflow!")
                        break;
                    }
         */
    }

    for (let line = 1; line < PARITY_FRAMES + 1; line++) {
        let parityMat = matrix_line(line, fragCount);
        console.log(parityMat);
        let xorbuff = Buffer.alloc(fragSize + 3, 0);
        let firstDone = 0;
        for (let i = 0; i < fragCount; i++) {
            if (parityMat[i] === 1) {
                if (firstDone === 1) {
                    for (let j = 0; j < fragSize; j++) {
                        xorbuff[j + 3] = xorbuff[j + 3] ^ packets[i][j + 3];
                    }
                    // console.log("XOR Operation: ", i);
                } else {
                    for (let j = 0; j < fragSize; j++) {
                        xorbuff[j + 3] = packets[i][j + 3];
                        firstDone = 1;
                    }
                    // console.log("First COPY: ", i);
                }
            }
        }

        let tmp = pktIdx | ((fragSesIdx << 14) & 0xC000);
        xorbuff[0] = 0x08;
        xorbuff[1] = (tmp & 0x00FF);
        xorbuff[2] = (tmp >> 8) & 0x00FF;

        packets.push(xorbuff);

        pktIdx++;
    }

    return packets;
}

async function startCountdownToStartClassCSession(startDelayInSec) {
    let minutesRemainding = startDelayInSec / 60;

    console.log('Starting countdown now...');

    for (let x = minutesRemainding; x > 0; x--) {
        console.log('Remaining time to start sending class C packets is %i minutes', x);
        await sleep(60000);
    }
}

function startSendingClassCPacketsForActiveDevices() {
	if (devices.some(eui => deviceMap[eui].fragSessionAns)) {
        startSendingClassCPackets();
	} else {
		throw new Error("FUOTA Session Timeout");
	}
}

async function startSendingClassCPackets() {
    classCStarted = true;
    console.log('startSendingClassCPackets');
    console.log('All devices ready?', deviceMap);
    console.log('fragCount = ', fragCount);

    let packets = parsePackets();

    let counter = 0;
    let pktIdx = 0;
    let fragSesIdx = 0;

    for (let p of packets) {

        pktIdx = (((p[2] << 8) & 0xFF00) | p[1]) & 0x3FFF;

        //if (pktIdx === 5 || pktIdx === 10)
        //continue;	//drop the packet

        let msg = {
            "downlinks": [{
                "priority": "NORMAL",
                "f_port": 201,
                "frm_payload": Buffer.from(p).toString('base64'),
                "class_b_c": {
                    "gateways": [{
                        "gateway_ids": {
                            "gateway_id": GATEWAY_ID
                        }
                    }]
                }
            }]
        };

        client.publish(`v3/${mcDetails.application_id+'@'+TENANT_NAME}/devices/${mcDetails.device_id}/down/push`, Buffer.from(JSON.stringify(msg), 'utf8'));

        console.log('Sent packet', ++counter, mcDetails.application_id + '@' + TENANT_NAME, mcDetails.device_id);

        if (pktIdx > fragCount) {
            await sleep(PARITY_FRAG_TX_DELAY); // tpacket on SF12 is 2100 ms. so this should just work
        } else {
            await sleep(FRAG_TX_DELAY); // tpacket on SF12 is 2100 ms. so this should just work
        }
    }

    console.log('Done sending all packets');
    console.log('Waiting for report gen');
    setTimeout(() => {
        try {
            console.log("FUOTA status report: ");
            devices.forEach(eui => {
                let dm = deviceMap[eui];
                console.log("-> ", eui, ": ", dm.fuotaDoneSuccessfully);
            });
            console.log("FUOTA completed");
        } catch (e) {
            console.error(e);
        }
    }, 20000);
}
